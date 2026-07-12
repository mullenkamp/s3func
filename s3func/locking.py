#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Distributed locks on S3-compatible object storage.

The lock is a Lamport-bakery-style election: each contender writes two small
ticket objects (its "ticket"), then repeatedly lists the lock prefix and yields
to older tickets. Storage only needs atomic single-object PUT/DELETE and a
listing — no compare-and-swap.

Hardening (0.9.0) against listing staleness and ticket deletion:
- self-visibility gate: a contender only trusts a listing once that listing
  shows its OWN ticket (the listing is then provably fresh w.r.t. its writes);
- confirming re-list: winning the election requires two consecutive clear
  listings separated by ``settle_delay`` seconds;
- own-ticket invariant: every decisive listing must still contain the
  contender's own ticket — if it disappears (e.g. ``break_other_locks`` by
  another client), acquisition raises instead of "winning" without a ticket;
- recovery via ``lock_id=`` restores the ticket but NOT the lock: ``acquire()``
  re-runs the full election (holding a ticket is not the same as having won).
"""
import uuid
import warnings
import weakref
from timeit import default_timer
from time import sleep
import urllib3
import datetime

from . import b2, s3


#######################################################
### Parameters


md5_locks = {'shared': 'cfcd208495d565ef66e7dff9f98764da', 'exclusive': 'c4ca4238a0b923820dcc509a6f75849b'}

## Election decision results (pure decision core)
ACQUIRED = 'acquired'
WAIT = 'wait'
TICKET_LOST = 'ticket_lost'

## Default age gate for break_other_locks: tickets younger than this are
## presumed to belong to a LIVE writer (ticket timestamps reflect session
## START, not activity - long bulk sessions are normal) and are not broken
## by a default-argument call. Override with an explicit timestamp.
default_break_age = 7200  # seconds (2 hours)

## Session registry: maps a service name to a session class. A callable may also
## be passed directly as the service (e.g. a fake session factory in tests, or a
## future CAS-capable provider session - see benchmarks/conditional_write_probe.py
## for the qualification gate a provider must pass before a CAS lock is added).
## (lambdas defer attribute access - s3/b2 import locking, so their session
## classes don't exist yet at this module's import time)
SESSION_REGISTRY = {
    's3': lambda **kw: s3.S3Session(**kw),
    'b2': lambda **kw: b2.B2Session(**kw),
}


#######################################################
### Functions


def init_session(service, session_kwargs):
    """
    Resolve a service name (via SESSION_REGISTRY) or callable to a session instance.
    """
    if callable(service):
        return service(**session_kwargs)
    cls = SESSION_REGISTRY.get(service)
    if cls is None:
        raise ValueError(f'{service} is not a service option.')
    return cls(**session_kwargs)


def release_lock(service, obj_lock_key, lock_id, version_ids, session_kwargs):
    """
    Made for the creation of finalize objects to ensure that the lock is released if something goes wrong.
    """
    del_dict = []
    for seq, version_id in version_ids.items():
        d = {'key': obj_lock_key + f'{lock_id}-{seq}'}
        if version_id is not None:
            d['version_id'] = version_id
        del_dict.append(d)

    if del_dict:
        session = init_session(service, session_kwargs)
        _ = session.delete_objects(del_dict)


def parse_lock_entries(objs, obj_lock_key_len):
    """
    Group raw listing entries into tickets: {lock_id: {'lock_type': str, 0: ts, 1: ts}}.
    """
    tickets = {}
    for l in objs:
        parts = l['key'][obj_lock_key_len:].split('-')
        if len(parts) != 2:
            continue
        lock_id, seq = parts
        if lock_id not in tickets:
            tickets[lock_id] = {'lock_type': l['lock_type']}
        tickets[lock_id][int(seq)] = l['upload_timestamp']
    return tickets


def ticket_is_older(timestamp_other, timestamp, lock_id, lock_id_other):
    """True if the other ticket wins the tiebreak (older timestamp; lexicographic id on ties)."""
    if timestamp_other == timestamp:
        return lock_id_other < lock_id
    return timestamp_other < timestamp


def evaluate_election(own_lock_id, exclusive, tickets):
    """
    Pure bakery election decision over a parsed listing snapshot.

    The contender's own timestamp is read FROM THE SNAPSHOT, never from its
    put-response: every contender must compare the exact same values (the
    listing's), or rounding differences between sources break the total order -
    live-observed on B2, where second-granularity listing timestamps tie while
    put-response timestamps differ, letting several contenders each conclude
    they are oldest (split-brain).

    Parameters
    ----------
    own_lock_id : str
    exclusive : bool
        The mode the contender requested. Exclusive waits on any older ticket;
        shared waits only on older exclusive tickets.
    tickets : dict
        As returned by parse_lock_entries (must include the contender's own
        ticket if it is visible).

    Returns ACQUIRED, WAIT, or TICKET_LOST. TICKET_LOST means the contender's
    own ticket is missing/incomplete in the snapshot - it cannot claim the lock
    (deleted by break_other_locks or never visible) and must raise or retry.
    """
    own = tickets.get(own_lock_id)
    if own is None or 0 not in own or 1 not in own:
        return TICKET_LOST

    own_timestamp = own[1]

    for other_id, other in tickets.items():
        if other_id == own_lock_id:
            continue
        if not exclusive and other['lock_type'] == 'shared':
            continue
        ts_other = other.get(1) or other.get(0)
        if ts_other is None:
            continue
        if ticket_is_older(ts_other, own_timestamp, own_lock_id, other_id):
            return WAIT

    return ACQUIRED


def init_lock(
    self, service: str, access_key_id: str, access_key: str, bucket: str, key: str, lock_id: str, session_kwargs,
    settle_delay: float = 1.0, visibility_timeout: float = 30.0,
):
    """ """
    self._session_kwargs = dict(access_key_id=access_key_id, access_key=access_key, bucket=bucket)
    self._session_kwargs.update(session_kwargs)

    self._service = service

    self._obj_lock_key = key + '.lock.'
    self._obj_lock_key_len = len(self._obj_lock_key)

    self._settle_delay = settle_delay
    self._visibility_timeout = visibility_timeout

    ## _acquired is only set by a completed, won election. A ticket existing in
    ## the remote (including one recovered via lock_id=) is NOT the lock.
    self._acquired = False
    self._ticket_exclusive = None

    # If lock_id was provided, check if it already exists to recover state
    if lock_id:
        session = init_session(service, self._session_kwargs)
        objs = self._list_objects(session, self._obj_lock_key, lock_id)
        self._version_ids = {0: None, 1: None}
        self._timestamp = None
        for obj in objs:
            obj_key_name = obj['key']
            if lock_id in obj_key_name:
                seq = int(obj_key_name[-1])
                self._version_ids[seq] = obj['version_id']
                self._ticket_exclusive = obj['lock_type'] == 'exclusive'
                if seq == 1:
                    self._timestamp = obj['upload_timestamp']
        if self._timestamp:
            self._finalizer = weakref.finalize(
                self, release_lock, service, self._obj_lock_key, lock_id, self._version_ids, self._session_kwargs
            )
            self.lock_id = lock_id
        else:
            raise ValueError(f'{lock_id} does not exist in the remote.')
    else:
        self._finalizer = None
        self._timestamp = None
        self._version_ids = {0: None, 1: None}
        self.lock_id = uuid.uuid4().hex[:13]

    self._key = key


######################################################
### Classes


class DistributedLock:
    """
    Base class for distributed locks using object storage. See the module
    docstring for the election algorithm and its hardening guarantees.
    """

    @staticmethod
    def _list_objects(session, obj_lock_key, lock_id=None):
        key = obj_lock_key + (lock_id if lock_id else "")

        # Try list_object_versions first; fall back to list_objects
        # for providers that don't support versioning (e.g. Mega S3)
        objs = session.list_object_versions(prefix=key)
        try:
            items = list(objs.iter_objects())
        except urllib3.exceptions.HTTPError as e:
            if '501' in str(e):
                objs = session.list_objects(prefix=key)
                items = list(objs.iter_objects())
            elif '401' in str(e) or '403' in str(e):
                raise
            else:
                raise

        res = []
        for l in items:
            ## Delete markers are release/deletion artifacts on versioned
            ## backends - never live tickets. Skip them explicitly.
            if l.get('delete_marker') or l.get('action') == 'hide':
                continue
            if not l.get('is_latest', True):
                continue
            if l.get('content_md5') == md5_locks['exclusive']:
                l['lock_type'] = 'exclusive'
            elif l.get('content_md5') == md5_locks['shared']:
                l['lock_type'] = 'shared'
            else:
                raise ValueError('This lock file was created by something else...')
            res.append(l)

        return res

    def _snapshot_tickets(self):
        session = init_session(self._service, self._session_kwargs)
        objs = self._list_objects(session, self._obj_lock_key)
        return parse_lock_entries(objs, self._obj_lock_key_len)

    def _put_lock_objects(self, exclusive):
        session = init_session(self._service, self._session_kwargs)
        body = b'1' if exclusive else b'0'

        for seq in (0, 1):
            obj_name = self._obj_lock_key + f'{self.lock_id}-{seq}'
            resp = session.put_object(obj_name, body)
            if resp.status == 200:
                self._version_ids[seq] = resp.metadata.get('version_id')
                if seq == 1:
                    self._timestamp = resp.metadata['upload_timestamp']
            else:
                release_lock(self._service, self._obj_lock_key, self.lock_id, self._version_ids, self._session_kwargs)
                raise urllib3.exceptions.HTTPError(f"Failed to put lock object: {resp.error}")

        self._ticket_exclusive = exclusive
        self._finalizer = weakref.finalize(
            self, release_lock, self._service, self._obj_lock_key, self.lock_id, self._version_ids, self._session_kwargs
        )

    def acquire(self, blocking=True, timeout=-1, exclusive=True):
        """
        Run the bakery election. Returns True once the lock is held, False on a
        non-blocking failure or timeout. Raises if this contender's own ticket
        disappears mid-election (e.g. removed by break_other_locks) or never
        becomes visible within ``visibility_timeout``.
        """
        if self._acquired:
            return True

        if self._timestamp is None:
            ## Fresh lock: write our ticket.
            self._put_lock_objects(exclusive)
        else:
            ## Recovered ticket (lock_id= recovery): the ticket exists but the
            ## election was never won by THIS instance - re-run it, in the
            ## ticket's actual mode.
            if self._ticket_exclusive is not None and exclusive != self._ticket_exclusive:
                raise ValueError(
                    f'This lock was recovered with a {"exclusive" if self._ticket_exclusive else "shared"} '
                    f'ticket, but acquire was called with exclusive={exclusive}.'
                )
            exclusive = self._ticket_exclusive if self._ticket_exclusive is not None else exclusive

        ## Self-visibility gate: do not evaluate the election until the listing
        ## reflects our own ticket (proves the listing is fresh w.r.t. our writes).
        gate_start = default_timer()
        while True:
            tickets = self._snapshot_tickets()
            own = tickets.get(self.lock_id)
            if own is not None and 0 in own and 1 in own:
                break
            if (default_timer() - gate_start) > self._visibility_timeout:
                self.release()
                raise urllib3.exceptions.HTTPError(
                    'Own lock objects did not become visible in the listing within '
                    f'{self._visibility_timeout}s - the listing cannot be trusted for an election.'
                )
            sleep(0.25)

        start_time = default_timer()
        while True:
            decision = evaluate_election(self.lock_id, exclusive, tickets)

            if decision == TICKET_LOST:
                self.release()
                raise urllib3.exceptions.HTTPError(
                    'This lock ticket disappeared during acquisition (broken or deleted by another client).'
                )

            if decision == ACQUIRED:
                ## Confirming re-list: require a second, independent clear
                ## listing after settle_delay before declaring victory.
                sleep(self._settle_delay)
                tickets = self._snapshot_tickets()
                confirm = evaluate_election(self.lock_id, exclusive, tickets)
                if confirm == TICKET_LOST:
                    self.release()
                    raise urllib3.exceptions.HTTPError(
                        'This lock ticket disappeared during acquisition (broken or deleted by another client).'
                    )
                if confirm == ACQUIRED:
                    self._acquired = True
                    return True
                ## A newly-visible older ticket - fall through to waiting.

            if not blocking:
                break

            if timeout > 0 and (default_timer() - start_time) > timeout:
                break

            sleep(1)
            tickets = self._snapshot_tickets()

        # Failed to acquire
        self.release()
        return False

    def release(self):
        self._acquired = False
        if self._timestamp is not None:
            if self._finalizer:
                self._finalizer()
            self._version_ids = {0: None, 1: None}
            self._timestamp = None
            self._ticket_exclusive = None

    def __enter__(self):
        if not self.acquire():
            raise urllib3.exceptions.HTTPError("Failed to acquire lock")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()

    def other_locks(self):
        session = init_session(self._service, self._session_kwargs)
        objs = self._list_objects(session, self._obj_lock_key)
        other_locks = {}
        for l in objs:
            parts = l['key'][self._obj_lock_key_len :].split('-')
            if len(parts) != 2:
                continue
            lock_id, seq = parts
            if lock_id != self.lock_id:
                other_locks[lock_id] = {
                    'upload_timestamp': l['upload_timestamp'],
                    'lock_type': l['lock_type'],
                    'owner': l.get('owner'),
                }
        return other_locks

    def break_other_locks(self, timestamp: str | datetime.datetime = None):
        """
        Delete OTHER contenders' lock tickets uploaded at or before a cutoff.

        By default the cutoff is ``default_break_age`` seconds (2 hours) ago:
        younger tickets are presumed to belong to a LIVE writer - ticket
        timestamps reflect session start, not activity, and long-running
        write sessions are normal. Breaking a live holder's ticket does not
        corrupt anything by itself, but the holder's next ``verify()`` will
        return False and it must abort its protected operation (losing that
        work). Pass an explicit ``timestamp`` to override the gate (e.g. now,
        to break everything regardless of age).

        The caller's OWN tickets are never deleted (before 0.9.3 the default
        cutoff was "now" and included them, killing the caller's own
        in-flight election).

        Returns the list of deleted ticket objects.
        """
        if timestamp is None:
            timestamp = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=default_break_age)
        elif isinstance(timestamp, str):
            timestamp = datetime.datetime.fromisoformat(timestamp).astimezone(datetime.timezone.utc)

        session = init_session(self._service, self._session_kwargs)
        objs = self._list_objects(session, self._obj_lock_key)
        keys = []
        own_prefix = self._obj_lock_key + self.lock_id + '-'
        for l in objs:
            if l['key'].startswith(own_prefix):
                continue
            if l['upload_timestamp'] <= timestamp:
                d = {'key': l['key']}
                if l.get('version_id'):
                    d['version_id'] = l['version_id']
                keys.append(d)
        if keys:
            session.delete_objects(keys)

        return keys

    def verify(self):
        """
        Re-verify that THIS instance still holds the lock.

        Returns True only if the election was won (``acquire()`` returned True
        and ``release()`` has not been called) AND a fresh listing still shows
        both of this instance's ticket objects. A False from a previous holder
        means the ticket was broken (another client's ``break_other_locks``) -
        mutual exclusion is gone and the protected operation must not proceed.

        Under listing staleness the failure direction is a spurious False
        (safe abort): the self-visibility gate already established that the
        listing showed this ticket once, so a listing that stops showing a
        long-since-written object is treated as a deletion signal.
        """
        if not self._acquired:
            return False

        session = init_session(self._service, self._session_kwargs)
        objs = self._list_objects(session, self._obj_lock_key, lock_id=self.lock_id)
        seqs = set()
        for l in objs:
            parts = l['key'][self._obj_lock_key_len:].split('-')
            if len(parts) == 2 and parts[0] == self.lock_id:
                seqs.add(parts[1])

        return '0' in seqs and '1' in seqs

    def locked(self):
        session = init_session(self._service, self._session_kwargs)
        return len(self._list_objects(session, self._obj_lock_key)) > 0


class S3Lock(DistributedLock):
    """
    S3 implementation of DistributedLock.
    """

    def __init__(
        self, access_key_id: str, access_key: str, bucket: str, key: str, lock_id: str = None,
        settle_delay: float = 1.0, visibility_timeout: float = 30.0, **session_kwargs
    ):
        init_lock(self, 's3', access_key_id, access_key, bucket, key, lock_id, session_kwargs,
                  settle_delay=settle_delay, visibility_timeout=visibility_timeout)


class B2Lock(DistributedLock):
    """
    B2-native implementation of DistributedLock.

    .. deprecated:: 0.9.0
        The B2 native API offers no capability or performance advantage over the
        S3-compatible endpoint. Use :class:`S3Lock` (via ``S3Session.lock``).
    """

    def __init__(
        self, access_key_id: str, access_key: str, bucket: str, key: str, lock_id: str = None,
        settle_delay: float = 1.0, visibility_timeout: float = 30.0, **session_kwargs
    ):
        warnings.warn(
            'B2Lock is deprecated and will be removed in s3func 1.0 - use S3Lock via the S3-compatible endpoint.',
            DeprecationWarning, stacklevel=2,
        )
        init_lock(self, 'b2', access_key_id, access_key, bucket, key, lock_id, session_kwargs,
                  settle_delay=settle_delay, visibility_timeout=visibility_timeout)
