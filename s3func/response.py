#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb  9 09:33:39 2026

@author: mike
"""
import orjson
import urllib3
import xml.etree.ElementTree as ET
import datetime
import copy
import urllib.parse

from s3func import utils


#############################################################
### Response classes



class Response:
    """
    Unified response class for S3, B2, and generic HTTP requests.
    """
    def __init__(self, response, stream_resp, service):
        """
        service: 's3', 'b2', or 'http'
        """
        self.status = response.status
        self.headers = dict(response.headers)
        self.error = None
        self.stream = None
        self.data = None
        self.metadata = {}

        # 1. Extract generic metadata from headers
        self.metadata = utils.add_metadata_from_urllib3(response)

        # 2. Extract service-specific metadata (e.g. from XML for S3)
        if service == 's3':
             meta_s3, err_s3 = utils.add_metadata_from_s3_xml(response)
             self.metadata.update(meta_s3)
             if err_s3:
                 self.error = err_s3
        elif service == 'b2':
             # B2 metadata from headers is already generic-ish (x-bz-info)
             # But if put_object, we might parse body JSON for more meta
             # Note: current B2Response just did add_metadata_from_urllib3 mostly
             pass

        # 3. Handle Body / Error
        if (self.status // 100) == 2:
            if stream_resp:
                self.stream = response
            else:
                self.data = response.data # Already read if preload_content=True
        else:
            # Error handling
            if service == 'b2':
                try:
                    self.error = orjson.loads(response.data)
                except:
                    self.error = {'status': self.status, 'message': 'The response produced nonsense content.'}
            elif service == 'http':
                 try:
                    self.error = orjson.loads(response.data)
                 except:
                    self.error = {'status': self.status, 'message': 'The response produced nonsense content.'}
            # S3 error is parsed in add_metadata_from_s3_xml

    def __repr__(self):
        return f'status: {self.status}'


# Aliases for backward compatibility during refactor
S3Response = lambda r, s: Response(r, s, 's3')
B2Response = lambda r, s: Response(r, s, 'b2')
HttpResponse = lambda r, s: Response(r, s, 'http')


def iter_s3_list(session, url, method, headers, params):
    """
    Iterates over S3 list results (Versions or Objects).
    """
    ns = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
    
    while True:
        # Create a fresh set of query params for each page
        # (params is modified in loop if truncated)
        
        # We need to re-sign each request because params change (markers)
        # S3Session.request handles signing, so we just pass updated params.
        
        # NOTE: This generator needs access to the 'request' method of the S3Session
        # passed as 'session' argument which we assume wraps the signing logic.
        
        resp = session.request('GET', url, headers=headers, fields=params)
        
        if (resp.status // 100) != 2:
            raise urllib3.exceptions.HTTPError(f"S3 List Error {resp.status}")

        try:
            root = ET.fromstring(resp.data)
        except ET.ParseError:
             raise urllib3.exceptions.HTTPError("Failed to parse S3 XML response")

        # Determine list type based on root tag or method
        # list_object_versions -> ListVersionsResult
        # list_objects_v2 -> ListBucketResult
        
        tag = root.tag.split('}')[-1] # Remove namespace
        
        if tag == 'ListVersionsResult':
            # Iterate Versions
            for version in root.findall('s3:Version', ns):
                yield {
                    # 'etag': version.find('s3:ETag', ns).text.strip('"') if version.find('s3:ETag', ns) is not None else None,
                    'content_md5': version.find('s3:ETag', ns).text.strip('"') if version.find('s3:ETag', ns) is not None else None,
                    'content_length': int(version.find('s3:Size', ns).text),
                    'key': version.find('s3:Key', ns).text,
                    'version_id': version.find('s3:VersionId', ns).text,
                    'is_latest': version.find('s3:IsLatest', ns).text == 'true',
                    'upload_timestamp': datetime.datetime.strptime(version.find('s3:LastModified', ns).text, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=datetime.timezone.utc),
                    # 'owner': ... (Owner might be present)
                }
            # Iterate DeleteMarkers if needed? (Not in original boto3 wrapper explicitly but handled)
            
            is_truncated = root.find('s3:IsTruncated', ns).text == 'true'
            if is_truncated:
                next_key_marker = root.find('s3:NextKeyMarker', ns)
                next_version_id_marker = root.find('s3:NextVersionIdMarker', ns)
                if next_key_marker is not None:
                    params['key-marker'] = next_key_marker.text
                if next_version_id_marker is not None:
                    params['version-id-marker'] = next_version_id_marker.text
            else:
                break
                
        elif tag == 'ListBucketResult': # V2
            for contents in root.findall('s3:Contents', ns):
                yield {
                    # 'etag': contents.find('s3:ETag', ns).text.strip('"'),
                    'content_md5': contents.find('s3:ETag', ns).text.strip('"'),
                    'content_length': int(contents.find('s3:Size', ns).text),
                    'key': contents.find('s3:Key', ns).text,
                    'upload_timestamp': datetime.datetime.strptime(contents.find('s3:LastModified', ns).text, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=datetime.timezone.utc),
                }
                
            is_truncated = root.find('s3:IsTruncated', ns).text == 'true'
            if is_truncated:
                token = root.find('s3:NextContinuationToken', ns)
                if token is not None:
                    params['continuation-token'] = token.text
            else:
                break
        else:
             # Fallback or error
             break


class S3ListResponse:
    """
    Wraps S3 listing logic using urllib3 and XML parsing.
    """
    def __init__(self, session, url, method, headers, params):
        self._session = session
        self._url = url
        self._method = method
        self._headers = headers
        self._params = params
        self.status = 200 # Assumed valid until iteration fails
        self.error = None

    def iter_objects(self):
        if self.error:
             raise urllib3.exceptions.HTTPError(self.error)
        return iter_s3_list(self._session, self._url, self._method, self._headers, copy.deepcopy(self._params))

    def __repr__(self):
        return f'status: {self.status}'


def iter_b2_list(session, url, headers, params):
    """

    """
    while True:
        resp = session.request('get', url, headers=headers, fields=params)
        data = orjson.loads(resp.data)

        if 'files' in data:
            for js in data['files']:
                if 'unverified:' in js['contentSha1']:
                    js['contentSha1'] = js['contentSha1'].split('unverified:')[1]
                dict1 = {
                    'action': js['action'],
                    'content_length': js['contentLength'],
                    'content_md5': js['contentMd5'],
                    'content_sha1': js['contentSha1'],
                    'content_type': js['contentType'],
                    'key': js['fileName'],
                    'version_id': js['fileId'],
                    'upload_timestamp': datetime.datetime.fromtimestamp(js['uploadTimestamp']*0.001, datetime.timezone.utc),
                    'owner': js['accountId'],
                    }
                if 'fileInfo' in js:
                    for fi, val in js['fileInfo'].items():
                        if fi == 'src_last_modified_millis':
                            dict1['last_modified'] = datetime.datetime.fromtimestamp(int(val)*0.001, datetime.timezone.utc)
                        else:
                            dict1[fi] = val

                yield dict1

            if data['nextFileName'] is None:
                break
            else:
                params['startFileName'] = data['nextFileName']
                if 'nextFileId' in data:
                    params['startFileId'] = data['nextFileId']
        else:
            break


class B2ListResponse:
    """

    """
    def __init__(self, request, session, api_url, headers, params):
        """

        """
        url = urllib.parse.urljoin(api_url, request)

        if 'maxFileCount' in params:
            max_keys = params['maxFileCount']
        else:
            max_keys = 10000

        params['maxFileCount'] = 1

        resp = session.request('get', url, headers=headers, fields=params)

        error = {}
        metadata = utils.add_metadata_from_urllib3(resp)
        # if objects:
        #     metadata['objects'] = objects

        if (resp.status // 100) != 2:
            try:
                error = orjson.loads(resp.data)
            except:
                error = {'status': resp.status, 'message': 'The response produced nonsense content.'}

        self.headers = dict(resp.headers)
        self.metadata = metadata
        self.stream = None
        self.error = error
        self.status = resp.status
        self._url = url
        self._session = session
        self._req_headers = headers

        params['maxFileCount'] = max_keys
        self._req_params = params


    # @property
    def iter_objects(self):
        """

        """
        if self.error:
            raise urllib3.exceptions.HTTPError(self.error)
        else:
            return iter_b2_list(self._session, self._url, self._req_headers, copy.deepcopy(self._req_params))


    def __repr__(self):
        """

        """
        return f'status: {self.status}'
















































