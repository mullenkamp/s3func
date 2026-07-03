"""
Deterministic lock tests: pure election core, hardening behavior under injected
listing staleness, recovery semantics, synthetic listing artifacts, and an
exhaustive small-model check. No network; runs in milliseconds.
"""
import datetime
import hashlib
import itertools

import pytest
import urllib3

from s3func import locking
from s3func.locking import (ACQUIRED, WAIT, TICKET_LOST, evaluate_election,
                            parse_lock_entries, DistributedLock, init_lock)

MD5_EXCL = locking.md5_locks['exclusive']
MD5_SHARED = locking.md5_locks['shared']


def ts(i):
    return datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc) + datetime.timedelta(seconds=i)


#################################################
### Pure election core


def T(lock_type='exclusive', t0=None, t1=None):
    d = {'lock_type': lock_type}
    if t0 is not None:
        d[0] = t0
    if t1 is not None:
        d[1] = t1
    return d


def test_election_basic():
    ## alone -> acquired
    assert evaluate_election('me', True, {'me': T(t0=ts(4), t1=ts(5))}) == ACQUIRED
    ## older exclusive competitor -> wait
    assert evaluate_election('me', True,
                             {'me': T(t0=ts(4), t1=ts(5)), 'a': T(t0=ts(1), t1=ts(2))}) == WAIT
    ## younger competitor -> acquired
    assert evaluate_election('me', True,
                             {'me': T(t0=ts(4), t1=ts(5)), 'a': T(t0=ts(8), t1=ts(9))}) == ACQUIRED


def test_election_own_ticket_lost():
    assert evaluate_election('me', True, {}) == TICKET_LOST
    ## partial ticket (one seq missing) is also lost
    assert evaluate_election('me', True, {'me': T(t1=ts(5))}) == TICKET_LOST
    assert evaluate_election('me', True, {'me': T(t0=ts(4))}) == TICKET_LOST


def test_election_tiebreak_lock_id():
    mine = {'me': T(t0=ts(1), t1=ts(5)), 'aa': T(t0=ts(1), t1=ts(5))}
    ## equal timestamps: lexicographically smaller id wins
    assert evaluate_election('me', True, mine) == WAIT      # 'aa' < 'me'
    assert evaluate_election('aa', True, mine) == ACQUIRED


def test_election_shared_semantics():
    tickets = {'w': T('exclusive', ts(4), ts(5)),
               'r1': T('shared', ts(1), ts(2)),
               'r2': T('shared', ts(2), ts(3))}
    ## shared contender ignores older shared and YOUNGER exclusives
    assert evaluate_election('r2', False, tickets) == ACQUIRED
    tickets_w_old = {'w': T('exclusive', ts(0), ts(1)), 'r2': T('shared', ts(2), ts(3))}
    assert evaluate_election('r2', False, tickets_w_old) == WAIT
    tickets_no_w = {'r1': T('shared', ts(1), ts(2)), 'r2': T('shared', ts(2), ts(3))}
    assert evaluate_election('r2', False, tickets_no_w) == ACQUIRED
    ## exclusive contender waits on older shared too
    assert evaluate_election('w', True,
                             {'w': T('exclusive', ts(4), ts(5)), 'r1': T('shared', ts(1), ts(2))}) == WAIT


def test_election_accumulation_regression():
    ## The pre-0.9.0 bug: only the LAST-iterated competitor counted. An older
    ## competitor must force WAIT regardless of dict insertion order.
    older = T(t0=ts(1), t1=ts(2))
    newer = T(t0=ts(8), t1=ts(9))
    me = T(t0=ts(4), t1=ts(5))
    assert evaluate_election('me', True, {'me': me, 'old': older, 'new': newer}) == WAIT
    assert evaluate_election('me', True, {'me': me, 'new': newer, 'old': older}) == WAIT


def test_election_shared_r2_fix():
    ## r2 (shared) with only a YOUNGER exclusive present acquires
    tickets = {'w': T('exclusive', ts(4), ts(5)), 'r2': T('shared', ts(2), ts(3))}
    assert evaluate_election('r2', False, tickets) == ACQUIRED


#################################################
### FakeSession: in-memory store with controllable staleness


class FakeStore:
    def __init__(self):
        self.objects = {}        # key -> dict entry
        self.counter = 0
        self.view_filters = {}   # tag -> callable(entries) -> entries

    def now(self):
        self.counter += 1
        return ts(self.counter)

    def put(self, key, body):
        t = self.now()
        entry = {
            'key': key,
            'content_md5': hashlib.md5(body).hexdigest(),
            'is_latest': True,
            'upload_timestamp': t,
            'version_id': f'v{self.counter}',
        }
        self.objects[key] = entry
        return entry

    def listing(self, prefix, tag=None):
        entries = [dict(e) for k, e in sorted(self.objects.items()) if k.startswith(prefix)]
        filt = self.view_filters.get(tag)
        if filt:
            entries = filt(entries)
        return entries


class FakeListResponse:
    def __init__(self, entries):
        self._entries = entries

    def iter_objects(self):
        return iter(self._entries)


class FakeResponse:
    def __init__(self, entry):
        self.status = 200
        self.metadata = {'version_id': entry['version_id'], 'upload_timestamp': entry['upload_timestamp']}
        self.error = None


class FakeSession:
    def __init__(self, store, tag=None, **kwargs):
        self.store = store
        self.tag = tag

    def put_object(self, key, body):
        return FakeResponse(self.store.put(key, body))

    def list_object_versions(self, prefix):
        return FakeListResponse(self.store.listing(prefix, self.tag))

    def list_objects(self, prefix):
        return FakeListResponse(self.store.listing(prefix, self.tag))

    def delete_objects(self, del_dicts):
        for d in del_dicts:
            self.store.objects.pop(d['key'], None)


class FakeLock(DistributedLock):
    def __init__(self, store, key, tag=None, lock_id=None, settle_delay=0.0, visibility_timeout=1.0):
        factory = lambda **kw: FakeSession(store, tag=tag)
        init_lock(self, factory, 'ak', 'sk', 'bucket', key, lock_id, {},
                  settle_delay=settle_delay, visibility_timeout=visibility_timeout)


@pytest.fixture(autouse=True)
def fast_sleep(monkeypatch):
    monkeypatch.setattr(locking, 'sleep', lambda s: None)


#################################################
### End-to-end deterministic scenarios


def test_basic_mutual_exclusion():
    store = FakeStore()
    a = FakeLock(store, 'k')
    b = FakeLock(store, 'k')
    assert a.acquire() is True
    assert b.acquire(blocking=False) is False
    a.release()
    assert b.acquire() is True
    b.release()
    assert store.listing('k.lock.') == []


def test_shared_locks_coexist_and_block_exclusive():
    store = FakeStore()
    r1 = FakeLock(store, 'k')
    r2 = FakeLock(store, 'k')
    w = FakeLock(store, 'k')
    assert r1.acquire(exclusive=False) is True
    assert r2.acquire(exclusive=False) is True
    assert w.acquire(blocking=False, exclusive=True) is False
    r1.release()
    r2.release()
    assert w.acquire() is True
    w.release()


def test_correlated_staleness_gate_blocks():
    ## B's listings are entirely stale (empty view): its own ticket never appears,
    ## so the visibility gate must refuse to run an election -> raise, no acquire.
    store = FakeStore()
    a = FakeLock(store, 'k')
    assert a.acquire() is True
    store.view_filters['B'] = lambda entries: []
    b = FakeLock(store, 'k', tag='B', visibility_timeout=0.0)
    with pytest.raises(urllib3.exceptions.HTTPError, match='did not become visible'):
        b.acquire()
    assert not b._acquired
    a.release()


def test_independent_staleness_caught_by_confirm():
    ## B sees its own ticket but a filter hides A's older ticket for B's first
    ## decisive listing only; the confirming re-list reveals A -> B must not win.
    store = FakeStore()
    a = FakeLock(store, 'k')
    assert a.acquire() is True

    calls = {'n': 0}

    def hide_a_once(entries):
        calls['n'] += 1
        if calls['n'] <= 1:   # the gate listing doubles as the first decisive listing
            return [e for e in entries if a.lock_id not in e['key']]
        return entries

    store.view_filters['B'] = hide_a_once
    b = FakeLock(store, 'k', tag='B')
    assert b.acquire(blocking=False) is False   # confirm re-list saw A -> WAIT -> non-blocking fail
    assert not b._acquired
    a.release()


def test_double_stale_reads_residual_window():
    ## Documented residual: if BOTH the decisive listing AND the confirming
    ## re-list independently miss the older ticket, the bakery cannot know.
    ## This test pins the boundary of the guarantee.
    store = FakeStore()
    a = FakeLock(store, 'k')
    assert a.acquire() is True
    store.view_filters['B'] = lambda entries: [e for e in entries if a.lock_id not in e['key']]
    b = FakeLock(store, 'k', tag='B')
    assert b.acquire() is True     # violation by construction - two stale reads
    assert a._acquired and b._acquired
    a.release()
    b.release()


def test_ticket_lost_raises():
    ## The victim's ticket is deleted (break_other_locks-style) mid-election ->
    ## acquire must raise, never win or spin forever.
    store = FakeStore()
    blocker = FakeLock(store, 'k')
    assert blocker.acquire() is True

    victim = FakeLock(store, 'k', tag='V')
    calls = {'n': 0}

    def steal_after_gate(entries):
        calls['n'] += 1
        if calls['n'] >= 2:   # let the gate see the ticket, then steal it
            for k in list(store.objects):
                if victim.lock_id in k:
                    del store.objects[k]
            return [e for e in entries if victim.lock_id not in e['key']]
        return entries

    store.view_filters['V'] = steal_after_gate
    with pytest.raises(urllib3.exceptions.HTTPError, match='disappeared'):
        victim.acquire()
    blocker.release()


def test_recovery_reruns_election():
    ## A ticket recovered via lock_id= must NOT be treated as a held lock: with
    ## an OLDER competitor present, acquire() must wait/fail, not return True.
    store = FakeStore()
    older = FakeLock(store, 'k')
    assert older.acquire() is True                 # older ticket, held

    orphan = FakeLock(store, 'k')
    orphan._put_lock_objects(True)                 # ticket written, election never run
    orphan_id = orphan.lock_id
    orphan._finalizer.detach()                     # simulate crash (no cleanup)

    recovered = FakeLock(store, 'k', lock_id=orphan_id)
    assert recovered._acquired is False
    assert recovered.acquire(blocking=False) is False   # must lose to `older`
    assert recovered._acquired is False

    older.release()


def test_recovery_mode_mismatch_raises():
    store = FakeStore()
    orphan = FakeLock(store, 'k')
    orphan._put_lock_objects(False)                # shared ticket
    orphan._finalizer.detach()
    recovered = FakeLock(store, 'k', lock_id=orphan.lock_id)
    with pytest.raises(ValueError, match='recovered with a shared'):
        recovered.acquire(exclusive=True)


#################################################
### Synthetic listing artifacts


def test_list_objects_skips_delete_markers_and_hide():
    entries = [
        {'key': 'k.lock.aaa-0', 'content_md5': MD5_EXCL, 'is_latest': True, 'upload_timestamp': ts(1)},
        {'key': 'k.lock.aaa-1', 'content_md5': MD5_EXCL, 'is_latest': True, 'upload_timestamp': ts(2)},
        {'key': 'k.lock.bbb-0', 'delete_marker': True, 'content_md5': None, 'is_latest': True, 'upload_timestamp': ts(3)},
        {'key': 'k.lock.ccc-0', 'action': 'hide', 'content_md5': None, 'upload_timestamp': ts(4)},
        {'key': 'k.lock.ddd-0', 'content_md5': MD5_SHARED, 'is_latest': False, 'upload_timestamp': ts(5)},
    ]
    session = FakeSession.__new__(FakeSession)
    session.store = None
    fake = FakeListResponse(entries)
    session.list_object_versions = lambda prefix: fake
    res = DistributedLock._list_objects(session, 'k.lock.')
    assert [e['key'] for e in res] == ['k.lock.aaa-0', 'k.lock.aaa-1']


def test_list_objects_raises_on_foreign_objects():
    entries = [{'key': 'k.lock.zzz-0', 'content_md5': 'deadbeef', 'is_latest': True, 'upload_timestamp': ts(1)}]
    session = FakeSession.__new__(FakeSession)
    fake = FakeListResponse(entries)
    session.list_object_versions = lambda prefix: fake
    with pytest.raises(ValueError, match='created by something else'):
        DistributedLock._list_objects(session, 'k.lock.')


#################################################
### Exhaustive small-model check
#
# Model: N contenders; each executes [put, decide-list, confirm-list].
# Every list op independently returns either the CURRENT store state or a state
# that is stale by up to K=1 preceding put operations. A contender "wins" iff
# its own ticket is visible in both listings (gate + own-ticket invariant) and
# evaluate_election returns ACQUIRED on both. Assertion: across ALL interleavings
# and staleness assignments, no forbidden pair wins - UNLESS both of a winner's
# listings were stale (the documented residual, excluded from the invariant).


def run_model(modes):
    n = len(modes)
    ids = [f'c{i}' for i in range(n)]
    put_orders = list(itertools.permutations(range(n)))
    stale_options = list(itertools.product([False, True], repeat=2 * n))

    violations = []
    for put_order in put_orders:
        ## store history: state after each put
        history = []
        state = {}
        put_time = {}
        for step, ci in enumerate(put_order):
            state = dict(state)
            state[ids[ci]] = {'lock_type': 'exclusive' if modes[ci] else 'shared',
                              0: ts(step * 2), 1: ts(step * 2 + 1)}
            put_time[ids[ci]] = ts(step * 2 + 1)
            history.append(state)
        final = history[-1]

        for stales in stale_options:
            winners = []
            residual = []
            for i, cid in enumerate(ids):
                pos = put_order.index(i)
                fresh = final
                ## a stale view for contender i: state as of one put earlier,
                ## but never earlier than its own put (the gate guarantees that)
                stale_view = history[max(pos, len(history) - 2)]
                v1 = stale_view if stales[2 * i] else fresh
                v2 = stale_view if stales[2 * i + 1] else fresh
                d1 = evaluate_election(cid, modes[i], v1)
                d2 = evaluate_election(cid, modes[i], v2)
                if d1 == ACQUIRED and d2 == ACQUIRED:
                    winners.append(i)
                    if stales[2 * i] and stales[2 * i + 1]:
                        residual.append(i)
            ## forbidden: two winners where at least one is exclusive, unless
            ## every "extra" winner won purely through double-stale reads
            exclusive_winners = [w for w in winners if modes[w]]
            if len(winners) > 1 and exclusive_winners:
                genuine = [w for w in winners if w not in residual]
                if len(genuine) > 1 and any(modes[w] for w in genuine):
                    violations.append((put_order, stales, winners))
    return violations


def test_model_two_exclusive():
    assert run_model([True, True]) == []


def test_model_exclusive_shared():
    assert run_model([True, False]) == []


def test_model_three_mixed():
    assert run_model([True, True, False]) == []
    assert run_model([True, False, False]) == []


def test_model_all_shared_coexist():
    ## sanity: three shared contenders may all win concurrently
    n = 3
    ids = [f'c{i}' for i in range(n)]
    state = {ids[i]: {'lock_type': 'shared', 0: ts(i * 2), 1: ts(i * 2 + 1)} for i in range(n)}
    for i in range(n):
        assert evaluate_election(ids[i], False, state) == ACQUIRED


def test_election_all_tied_single_winner():
    ## Second-granularity listing timestamps (observed on B2) tie all tickets:
    ## the id tiebreak must yield EXACTLY ONE exclusive winner.
    ids = ['c1', 'c2', 'c3', 'c4']
    tickets = {i: {'lock_type': 'exclusive', 0: ts(0), 1: ts(0)} for i in ids}
    results = [evaluate_election(i, True, tickets) for i in ids]
    assert results.count(ACQUIRED) == 1 and results.count(WAIT) == 3
    assert results[0] == ACQUIRED  # lexicographically smallest id wins
