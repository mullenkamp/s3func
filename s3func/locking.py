#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb  9 09:16:01 2026

@author: mike
"""
import uuid
import weakref
from timeit import default_timer
from time import sleep
import urllib3
import datetime

from . import b2, s3


#######################################################
### Parameters


md5_locks = {'shared': 'cfcd208495d565ef66e7dff9f98764da', 'exclusive': 'c4ca4238a0b923820dcc509a6f75849b'}


#######################################################
### Functions


def init_session(service, session_kwargs):
    """ """
    if service == 's3':
        session = s3.S3Session(**session_kwargs)
    elif service == 'b2':
        session = b2.B2Session(**session_kwargs)
    else:
        raise ValueError(f'{service} is not a service option.')

    return session


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


def init_lock(
    self, service: str, access_key_id: str, access_key: str, bucket: str, key: str, lock_id: str, session_kwargs
):
    """ """
    self._session_kwargs = dict(access_key_id=access_key_id, access_key=access_key, bucket=bucket)
    self._session_kwargs.update(session_kwargs)

    self._service = service

    self._obj_lock_key = key + '.lock.'
    self._obj_lock_key_len = len(self._obj_lock_key)

    # If lock_id was provided, check if it already exists to recover state
    if lock_id:
        session = init_session(service, self._session_kwargs)
        objs = self._list_objects(session, self._obj_lock_key, lock_id)
        for obj in objs:
            obj_key_name = obj['key']
            if lock_id in obj_key_name:
                seq = int(obj_key_name[-1])
                self._version_ids[seq] = obj['version_id']
                if seq == 1:
                    self._timestamp = obj['upload_timestamp']
        if self._timestamp:
            self._finalizer = weakref.finalize(
                self, release_lock, service, self._obj_lock_key, self.lock_id, self._version_ids, self._session_kwargs
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
    Base class for distributed locks using object storage.
    """

    # def __init__(self, service: str, access_key_id: str, access_key: str, bucket: str, key: str, endpoint_url: str=None, lock_id: str=None, **session_kwargs):
    #     self._session_kwargs = dict(access_key_id=access_key_id, access_key=access_key, bucket=bucket, endpoint_url=endpoint_url)
    #     self._session_kwargs.update(session_kwargs)

    #     self._service = service

    #     self._obj_lock_key = key + '.lock.'
    #     self._obj_lock_key_len = len(self._obj_lock_key)

    #     # If lock_id was provided, check if it already exists to recover state
    #     if lock_id:
    #         session = init_session(service, self._session_kwargs)
    #         objs = self._list_objects(session, self._obj_lock_key, lock_id)
    #         for obj in objs:
    #             obj_key_name = obj['key']
    #             if lock_id in obj_key_name:
    #                 seq = int(obj_key_name[-1])
    #                 self._version_ids[seq] = obj['version_id']
    #                 if seq == 1:
    #                     self._timestamp = obj['upload_timestamp']
    #         if self._timestamp:
    #             self._finalizer = weakref.finalize(self, release_lock, service, self._obj_lock_key, self.lock_id, self._version_ids, self._session_kwargs)
    #             self.lock_id = lock_id
    #         else:
    #             raise ValueError(f'{lock_id} does not exist in the remote.')
    #     else:
    #         self._finalizer = None
    #         self._timestamp = None
    #         self._version_ids = {0: None, 1: None}
    #         self.lock_id = uuid.uuid4().hex[:13]

    #     self._key = key

    @staticmethod
    def _list_objects(session, obj_lock_key, lock_id=None):
        key = obj_lock_key + (lock_id if lock_id else "")
        objs = session.list_object_versions(prefix=key)

        if objs.status in (401, 403):
            raise urllib3.exceptions.HTTPError(str(objs.error))

        res = []
        for l in objs.iter_objects():
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

    def _check_older_timestamp(self, timestamp_other, timestamp, lock_id, lock_id_other):
        if timestamp_other == timestamp:
            return lock_id_other < lock_id
        return timestamp_other < timestamp

    def _check_for_older_objs(self, objs, exclusive=False):
        locked = False
        for lock_id_other, obj in objs.items():
            if not exclusive and obj.get('lock_type') == 'shared':
                continue

            # Use timestamp from seq 1 if available, else seq 0
            ts_other = obj.get(1) or obj.get(0)
            locked = self._check_older_timestamp(ts_other, self._timestamp, self.lock_id, lock_id_other)

        return locked

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
                # self._timestamp = resp.metadata['upload_timestamp']
            else:
                release_lock(self._service, self._obj_lock_key, self.lock_id, self._version_ids, self._session_kwargs)
                raise urllib3.exceptions.HTTPError(f"Failed to put lock object: {resp.error}")

        self._finalizer = weakref.finalize(
            self, release_lock, self._service, self._obj_lock_key, self.lock_id, self._version_ids, self._session_kwargs
        )

    def _list_other_locks(self):
        session = init_session(self._service, self._session_kwargs)
        objs = self._list_objects(session, self._obj_lock_key)
        other_locks = {}
        for l in objs:
            parts = l['key'][self._obj_lock_key_len :].split('-')
            if len(parts) != 2:
                continue
            lock_id, seq = parts
            if lock_id != self.lock_id:
                if lock_id not in other_locks:
                    other_locks[lock_id] = {'lock_type': l['lock_type']}
                other_locks[lock_id][int(seq)] = l['upload_timestamp']
        return other_locks

    def acquire(self, blocking=True, timeout=-1, exclusive=True):
        if self._timestamp is not None:
            return True

        self._put_lock_objects(exclusive)

        start_time = default_timer()
        while True:
            objs = self._list_other_locks()
            locked = self._check_for_older_objs(objs, exclusive)

            if not locked:
                return True

            if not blocking:
                break

            if timeout > 0 and (default_timer() - start_time) > timeout:
                break

            sleep(2)

        # Failed to acquire
        self.release()
        return False

    def release(self):
        if self._timestamp is not None:
            if self._finalizer:
                self._finalizer()
            self._version_ids = {0: None, 1: None}
            self._timestamp = None

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
        if timestamp is None:
            timestamp = datetime.datetime.now(datetime.timezone.utc)
        elif isinstance(timestamp, str):
            timestamp = datetime.datetime.fromisoformat(timestamp).astimezone(datetime.timezone.utc)

        session = init_session(self._service, self._session_kwargs)
        objs = self._list_objects(session, self._obj_lock_key)
        keys = []
        for l in objs:
            if l['upload_timestamp'] <= timestamp:
                keys.append({'key': l['key'], 'version_id': l['version_id']})
        if keys:
            session.delete_objects(keys)

        return keys

    def locked(self):
        session = init_session(self._service, self._session_kwargs)
        return len(self._list_objects(session, self._obj_lock_key)) > 0


class S3Lock(DistributedLock):
    """
    S3 implementation of DistributedLock.
    """

    def __init__(
        self, access_key_id: str, access_key: str, bucket: str, key: str, lock_id: str = None, **session_kwargs
    ):
        init_lock(self, 's3', access_key_id, access_key, bucket, key, lock_id, session_kwargs)


class B2Lock(DistributedLock):
    """
    B2 implementation of DistributedLock.
    """

    def __init__(
        self, access_key_id: str, access_key: str, bucket: str, key: str, lock_id: str = None, **session_kwargs
    ):
        init_lock(self, 'b2', access_key_id, access_key, bucket, key, lock_id, session_kwargs)
