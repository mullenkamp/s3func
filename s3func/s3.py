#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on May 13 08:04:38 2024

@author: mike
"""
import io
from typing import List, Union

# import boto3
# import botocore
import copy
import hashlib
import urllib3
import urllib.parse
import uuid
import xml.etree.ElementTree as ET
from time import sleep
from timeit import default_timer
import datetime
import weakref
import base64

from . import http_url, utils, response, locking

# import http_url, utils, response, locking


#######################################################
### Parameters


md5_locks = {'shared': 'cfcd208495d565ef66e7dff9f98764da', 'exclusive': 'c4ca4238a0b923820dcc509a6f75849b'}


#######################################################
### Functions


#######################################################
### Other classes


# class S3UserMetadata:
#     """

#     """
#     def __init__(self,


#######################################################
### Main class


class S3Session:
    """ """

    def __init__(
        self,
        access_key_id: str,
        access_key: str,
        bucket: str,
        endpoint_url: str = None,
        region: str = 'us-east-1',
        max_pool_connections: int = 10,
        max_attempts: int = 3,
        retry_mode: str = 'adaptive',
        read_timeout: int = 120,
        stream=True,
    ):
        """
        Establishes an S3 client connection with an S3 account.

        Parameters
        ----------
        access_key_id : str
            The access key id also known as aws_access_key_id.
        access_key : str
            The access key also known as aws_secret_access_key.
        bucket : str
            The bucket to be used when performing S3 operations.
        endpoint_url : str
            The nedpoint http(s) url for the s3 service.
        region : str
            The AWS region. Default us-east-1.
        max_pool_connections : int
            The number of simultaneous connections for the S3 connection.
        max_attempts: int
            The number of max attempts passed to the "retries" option in the S3 config.
        retry_mode: str
            The retry mode passed to the "retries" option in the S3 config.
        read_timeout: int
            The read timeout in seconds passed to the "retries" option in the S3 config.
        stream : bool
            Should the connection stay open for streaming or should all the data/content be loaded during the initial request.
        """
        self._session = http_url.session(max_pool_connections, max_attempts, read_timeout)

        from .signer import SigV4Auth

        self._signer = SigV4Auth(access_key_id, access_key, region)

        self.bucket = bucket
        self._stream = stream
        self._endpoint_url = endpoint_url
        if self._endpoint_url and not self._endpoint_url.endswith('/'):
            self._endpoint_url += '/'
        # Default endpoint if not provided? Assume standard AWS
        if not self._endpoint_url:
            self._endpoint_url = f'https://s3.{region}.amazonaws.com/'

        self._access_key_id = access_key_id
        self._access_key = access_key
        self._max_attempts = max_attempts
        self._retry_mode = retry_mode
        self._read_timeout = read_timeout

    def _object_url(self, key, bucket=None):
        """
        Build an object URL with the key percent-encoded exactly once. SigV4's
        canonical URI is the encoded path, and servers canonicalize what they
        receive - an unencoded special character (e.g. '!') on the wire makes
        the two disagree and fails signature validation.
        """
        b = bucket or self.bucket
        return urllib.parse.urljoin(self._endpoint_url, f"{b}/{urllib.parse.quote(key)}")

    def request(self, method, url, headers=None, fields=None, body=None, preload_content=None):
        """
        Wrapper to perform signed request via urllib3
        """
        if headers is None:
            headers = {}

        if preload_content is None:
            preload_content = not self._stream

        # Sign request
        if fields:
            scheme, netloc, path, query, fragment = urllib.parse.urlsplit(url)
            ## RFC3986 encoding (space -> %20, not '+'): the SigV4 canonical query
            ## uses %20, and not every provider canonicalizes a wire '+' as a
            ## space (MEGA S4 does not) - identical wire/canonical encoding is
            ## the only portable choice.
            query_str = urllib.parse.urlencode(fields, quote_via=urllib.parse.quote)
            if query:
                query = f"{query}&{query_str}"
            else:
                query = query_str
            url = urllib.parse.urlunsplit((scheme, netloc, path, query, fragment))
            fields = None  # Cleared so urllib3 doesn't add them again

        self._signer.add_auth(method, url, headers, body)

        return self._session.request(method, url, headers=headers, body=body, preload_content=preload_content)

    def get_object(self, key: str, version_id: str = None, range_start: int = None, range_end: int = None):
        """
        Method to get an object from an S3 bucket.

        Parameters
        ----------
        key : str
            The object key in the S3 bucket.
        version_id : str
            The S3 version id associated with the object.
        range_start: int
            The byte range start for the file.
        range_end: int
            The byte range end for the file.
        chunk_size: int
            The amount of bytes to download as once.

        Returns
        -------
        S3Response
        """
        url = self._object_url(key)

        query_params, headers = utils.build_s3_params(
            self.bucket, key=key, version_id=version_id, range_start=range_start, range_end=range_end
        )

        # For get_object, we respect the session's stream setting
        resp = self.request('GET', url, headers=headers, fields=query_params)

        s3resp = response.S3Response(resp, self._stream)

        return s3resp

    def head_object(self, key: str, version_id: str = None):
        """
        Method to get the headers/metadata of an object from an S3 bucket.

        Parameters
        ----------
        key : str
            The object key in the S3 bucket.
        version_id : str
            The S3 version id associated with the object.

        Returns
        -------
        S3Response
        """
        url = self._object_url(key)

        query_params, headers = utils.build_s3_params(self.bucket, key=key, version_id=version_id)

        # head_object never has a body, safe to preload
        resp = self.request('HEAD', url, headers=headers, fields=query_params, preload_content=True)

        s3resp = response.S3Response(resp, False)

        return s3resp

    def put_object(self, key: str, obj: Union[bytes, io.BufferedIOBase], metadata: dict = {}, content_type: str = None):
        """
        Method to upload data to an S3 bucket.

        Parameters
        ----------
        key : str
            The key name for the uploaded object.
        obj : bytes, io.BytesIO, or io.BufferedIOBase
            The file object to be uploaded.
        metadata : dict or None
            A dict of the user metadata that should be saved along with the object. Keys and values must be strings. User-metadata must be under 2048 bytes of string encoded data.
        content_type : str
            The http content type to associate the object with.

        Returns
        -------
        S3Response
        """
        # TODO : In python version 3.11, the file_digest function can input a file object

        if isinstance(obj, (bytes, bytearray)) and ('content-md5' not in metadata):
            # S3 usually expects base64 encoded MD5 for Content-MD5 header check
            # But here we just want to ensure integrity or metadata?
            # Boto3 does this. urllib3 doesn't automatically.
            # self.request will handle signing with SHA256.
            pass

        # Check for metadata size
        size = 0
        for meta_key, meta_val in metadata.items():
            if isinstance(meta_key, str) and isinstance(meta_val, str):
                size += len(meta_key.encode())
                size += len(meta_val.encode())
            else:
                raise TypeError('metadata keys and values must be strings.')

        if size > 2048:
            raise ValueError('metadata size is {size} bytes, but it must be under 2048 bytes.')

        url = self._object_url(key)

        query_params, headers = utils.build_s3_params(
            self.bucket, key=key, metadata=metadata, content_type=content_type
        )

        # Set Content-Length if possible
        if isinstance(obj, (bytes, bytearray)):
            headers['Content-Length'] = str(len(obj))
        elif hasattr(obj, 'seek') and hasattr(obj, 'tell'):
            curr = obj.tell()
            obj.seek(0, io.SEEK_END)
            headers['Content-Length'] = str(obj.tell() - curr)
            obj.seek(curr)

        # PutObject response is always small, safe to preload
        resp = self.request('PUT', url, headers=headers, fields=query_params, body=obj, preload_content=True)

        s3resp = response.S3Response(resp, False)
        s3resp.metadata.update(metadata)

        return s3resp

    def list_objects(self, prefix: str = None, start_after: str = None, delimiter: str = None, max_keys: int = None):
        """
        Wrapper S3 method around the list_objects_v2 client function.

        Parameters
        ----------
            Limits the response to keys that begin with the specified prefix.
        start_after : str
            The S3 key to start after.
        delimiter : str
            A delimiter is a character you use to group keys.
        max_keys : int
            Sets the maximum number of keys returned in the response. By default, the action returns up to 1,000 key names. The response might contain fewer keys but will never contain more.

        Returns
        -------
        S3ListResponse
        """
        url = urllib.parse.urljoin(self._endpoint_url, f"{self.bucket}")
        # List v2 uses ?list-type=2
        query_params, headers = utils.build_s3_params(
            self.bucket, start_after=start_after, prefix=prefix, delimiter=delimiter, max_keys=max_keys
        )
        query_params['list-type'] = '2'

        # Listing always needs to read XML response
        resp = response.S3ListResponse(self, url, 'GET', headers, query_params)

        return resp

    def list_object_versions(
        self, prefix: str = None, start_after: str = None, delimiter: str = None, max_keys: int = None
    ):
        """
        Wrapper S3 method around the list_object_versions client function.

        Parameters
        ----------
        prefix : str
            Limits the response to keys that begin with the specified prefix.
        start_after : str
            The S3 key to start after.
        delimiter : str or None
            A delimiter is a character you use to group keys.
        max_keys : int
            Sets the maximum number of keys returned in the response. By default, the action returns up to 1,000 key names. The response might contain fewer keys but will never contain more.

        Returns
        -------
        S3ListResponse
        """
        url = urllib.parse.urljoin(self._endpoint_url, f"{self.bucket}")
        query_params, headers = utils.build_s3_params(
            self.bucket, key_marker=start_after, prefix=prefix, delimiter=delimiter, max_keys=max_keys
        )
        query_params['versions'] = ''  # versions subresource

        # Listing always needs to read XML response
        resp = response.S3ListResponse(self, url, 'GET', headers, query_params)

        return resp

    def delete_object(self, key: str, version_id: str = None):
        """
        Delete a single object/version.

        Parameters
        ----------
        key : str
            The object key in the S3 bucket.
        version_id : str
            The S3 version id associated with the object.

        Returns
        -------
        S3Response
        """
        url = self._object_url(key)

        query_params, headers = utils.build_s3_params(self.bucket, key=key, version_id=version_id)

        # delete_object response is small
        resp = self.request('DELETE', url, headers=headers, fields=query_params, preload_content=True)

        s3resp = response.S3Response(resp, False)

        return s3resp

    def delete_objects(self, keys: Union[List[str], List[dict]] = None, prefix: str = None, purge: bool = True):
        """
        Delete multiple objects from an S3 bucket.

        Parameters
        ----------
        keys : list of str or list of dict, optional
            Either a list of key strings (e.g. ['foo.bin', 'bar.bin']) or a list of
            dicts with 'key' and optionally 'version_id' fields.
            Mutually exclusive with prefix.
        prefix : str, optional
            Delete all objects matching this prefix. Mutually exclusive with keys.
        purge : bool
            If True (default), all versions of each object are deleted by listing
            versions first. If False, objects are deleted without version IDs
            (which only adds a delete marker on versioned buckets).

        Returns
        -------
        None
        """
        if keys is not None and prefix is not None:
            raise ValueError('keys and prefix are mutually exclusive.')
        if keys is None and prefix is None:
            raise ValueError('Either keys or prefix must be provided.')

        if prefix is not None:
            # List all objects under the prefix and collect for deletion
            if purge:
                try:
                    resp = self.list_object_versions(prefix=prefix)
                    items = list(resp.iter_objects())
                except urllib3.exceptions.HTTPError as e:
                    if '501' in str(e):
                        resp = self.list_objects(prefix=prefix)
                        items = list(resp.iter_objects())
                    else:
                        raise
            else:
                resp = self.list_objects(prefix=prefix)
                items = list(resp.iter_objects())
            keys = [{'key': obj['key'], 'version_id': obj.get('version_id')} for obj in items]
        else:
            # Normalize input: strings -> dicts
            if keys and isinstance(keys[0], str):
                keys = [{'key': k} for k in keys]

            # If purge, resolve version IDs for keys that don't have them
            if purge:
                resolved = []
                for k in keys:
                    key_name = k.get('key') or k.get('Key')
                    version_id = k.get('version_id') or k.get('VersionId')
                    if version_id:
                        resolved.append(k)
                    else:
                        try:
                            resp = self.list_object_versions(prefix=key_name)
                            found = False
                            for obj in resp.iter_objects():
                                if obj['key'] == key_name:
                                    resolved.append({'key': obj['key'], 'version_id': obj['version_id']})
                                    found = True
                            if not found:
                                resolved.append(k)
                        except urllib3.exceptions.HTTPError as e:
                            if '501' in str(e):
                                resp = self.list_objects(prefix=key_name)
                                found = False
                                for obj in resp.iter_objects():
                                    if obj['key'] == key_name:
                                        resolved.append({'key': obj['key']})
                                        found = True
                                if not found:
                                    resolved.append(k)
                            else:
                                resolved.append(k)
                        except Exception:
                            resolved.append(k)
                keys = resolved

        # S3 Delete Objects requires a specific XML payload
        # <Delete><Object><Key>...</Key><VersionId>...</VersionId></Object>...</Delete>
        url = urllib.parse.urljoin(self._endpoint_url, f"{self.bucket}")

        for keys_chunk in utils.chunks(keys, 1000):
            # Build XML
            root = ET.Element('Delete', xmlns="http://s3.amazonaws.com/doc/2006-03-01/")
            quiet = ET.SubElement(root, 'Quiet')
            quiet.text = 'true'

            for k in keys_chunk:
                obj = ET.SubElement(root, 'Object')
                if 'key' in k:
                    ET.SubElement(obj, 'Key').text = k['key']
                elif 'Key' in k:
                    ET.SubElement(obj, 'Key').text = k['Key']
                else:
                    raise ValueError('"key" must be passed in the list of dict.')

                if k.get('version_id'):
                    ET.SubElement(obj, 'VersionId').text = k['version_id']
                elif k.get('VersionId'):
                    ET.SubElement(obj, 'VersionId').text = k['VersionId']

            body = ET.tostring(root, encoding='utf-8')

            query_params = {'delete': ''}

            md5 = base64.b64encode(hashlib.md5(body).digest()).decode('utf-8')
            headers = {'Content-MD5': md5, 'Content-Type': 'application/xml'}

            self.request('POST', url, headers=headers, fields=query_params, body=body, preload_content=True)

    def copy_object(
        self,
        source_key: str,
        dest_key: str,
        source_version_id: str | None = None,
        source_bucket: str | None = None,
        dest_bucket: str | None = None,
        metadata: dict = {},
        content_type: str = None,
    ):
        """
        Copy an object within S3. The source and destination must use the same credentials.

        Parameters
        ----------
        source_key : str
            The source key
        dest_key : str
            The destination key
        source_version_id : str or None
            The specific version id of the source object. Defaults to None.
        source_bucket : str or None
            The source bucket. If None, then it uses the initialised bucket.
        dest_bucket: str or None
            The destimation bucket. If None, then it uses the initialised bucket.
        metadata : dist
            The metadata for the destination object. If no metadata is provided, then the metadata is copied from the source.

        Returns
        -------
        S3Response
        """
        # Destination
        if dest_bucket is None:
            dest_bucket = self.bucket
        url = self._object_url(dest_key, bucket=dest_bucket)

        # Source header: x-amz-copy-source: /bucket/key?versionId=...
        if source_bucket is None:
            source_bucket = self.bucket

        copy_source = f"{source_bucket}/{urllib.parse.quote(source_key)}"
        if source_version_id:
            copy_source += f"?versionId={source_version_id}"

        # Headers
        headers = {'x-amz-copy-source': copy_source}

        if metadata:
            # Check for metadata size
            size = 0
            for meta_key, meta_val in metadata.items():
                if isinstance(meta_key, str) and isinstance(meta_val, str):
                    size += len(meta_key.encode())
                    size += len(meta_val.encode())
                else:
                    raise TypeError('metadata keys and values must be strings.')

            if size > 2048:
                raise ValueError('metadata size is {size} bytes, but it must be under 2048 bytes.')

            for k, v in metadata.items():
                headers[f'x-amz-meta-{k}'] = v
            headers['x-amz-metadata-directive'] = 'REPLACE'

        if isinstance(content_type, str):
            headers['Content-Type'] = content_type

        # CopyObject response contains XML, safe to preload
        resp = self.request('PUT', url, headers=headers, preload_content=True)

        s3resp = response.S3Response(resp, False)
        s3resp.metadata.update(metadata)

        return s3resp

    ########################################################
    ### S3 Locks and holds

    def get_object_lock_config(self):
        """
        Function to determine if a bucket is configured to have object locks. The user must have s3:GetBucketObjectLockConfiguration or b2:readBucketRetentions permissions for this request.

        Returns
        -------
        S3Reponse
        """
        url = urllib.parse.urljoin(self._endpoint_url, f"{self.bucket}")
        query_params = {'object-lock': ''}

        resp = self.request('GET', url, fields=query_params, preload_content=True)
        s3resp = response.S3Response(resp, False)

        return s3resp

    def put_object_lock_config(self, lock: bool = False):
        """
        Function to enable or disable object locks for a bucket. The user must have s3:PutBucketObjectLockConfiguration or b2:writeBucketRetentions permissions for this request.

        Parameters
        ----------
        lock : bool
            Should a lock be enabled for the bucket?

        Returns
        -------
        boto3 response
        """
        status = 'Enabled' if lock else 'Disabled'  # 'Enable' or 'Enabled'? AWS spec says 'Enabled'.

        body = f'<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><ObjectLockEnabled>{status}</ObjectLockEnabled></ObjectLockConfiguration>'

        md5 = base64.b64encode(hashlib.md5(body.encode('utf-8')).digest()).decode('utf-8')
        headers = {'Content-MD5': md5, 'Content-Type': 'application/xml'}

        url = urllib.parse.urljoin(self._endpoint_url, f"{self.bucket}")
        query_params = {'object-lock': ''}

        resp = self.request('PUT', url, headers=headers, fields=query_params, body=body, preload_content=True)

        s3resp = response.S3Response(resp, False)
        return s3resp

    def lock(self, key: str, lock_id: str = None, **lock_kwargs):
        """
        This class contains a locking mechanism by utilizing S3 objects. It has implementations for both shared and exclusive (the default) locks. It follows the same locking API as python thread locks (https://docs.python.org/3/library/threading.html#lock-objects), but with some extra methods for managing "deadlocks". The required S3 permissions are ListObjects, WriteObjects, and DeleteObjects.

        This initialized class can be used as a context manager exactly like thread locks.

        Parameters
        ----------
        key : str
            The base object key that will be given a lock. The extension ".lock" plus a unique lock id will be appended to the key, so the user is welcome to reference an existing object without worry that it will be overwritten or deleted.
        lock_id: str or None
            Reuse an existing lock ID. Defaults to none which will create a new ID.
        **lock_kwargs
            Passed through to S3Lock - e.g. the hardening knobs ``settle_delay``
            (default 1.0s) and ``visibility_timeout`` (default 30s).

        Returns
        -------
        DistributedLock
        """
        return locking.S3Lock(
            self._access_key_id,
            self._access_key,
            self.bucket,
            key,
            lock_id,
            endpoint_url=self._endpoint_url,
            max_attempts=self._max_attempts,
            retry_mode=self._retry_mode,
            read_timeout=self._read_timeout,
            **lock_kwargs,
        )
