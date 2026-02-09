import hmac
import hashlib
import datetime
import urllib.parse
from typing import Dict, Optional, Union

def sign(key, msg):
    return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()

def getSignatureKey(key, dateStamp, regionName, serviceName):
    kDate = sign(('AWS4' + key).encode('utf-8'), dateStamp)
    kRegion = sign(kDate, regionName)
    kService = sign(kRegion, serviceName)
    kSigning = sign(kService, 'aws4_request')
    return kSigning

class SigV4Auth:
    def __init__(self, access_key: str, secret_key: str, region: str, service: str = 's3'):
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.service = service

    def add_auth(self, request_method: str, url: str, headers: Dict[str, str], body: Union[bytes, str, None] = None):
        """
        Calculates the AWS Signature Version 4 and adds the Authorization header to the headers dict.
        """
        # Parse URL
        parsed_url = urllib.parse.urlparse(url)
        host = parsed_url.netloc
        path = parsed_url.path or '/'
        
        # Canonical Query String
        query_string = parsed_url.query
        canonical_query_string = ''
        if query_string:
            params = urllib.parse.parse_qs(query_string, keep_blank_values=True)
            sorted_params = sorted(params.items())
            canonical_query_parts = []
            for k, v_list in sorted_params:
                # Use strict RFC 3986 encoding (space as %20 not +)
                encoded_k = urllib.parse.quote(k, safe='-_.~')
                for v in sorted(v_list):
                    encoded_v = urllib.parse.quote(v, safe='-_.~')
                    canonical_query_parts.append(f"{encoded_k}={encoded_v}")
            canonical_query_string = '&'.join(canonical_query_parts)

        # Time
        t = datetime.datetime.now(datetime.timezone.utc)
        amz_date = t.strftime('%Y%m%dT%H%M%SZ')
        datestamp = t.strftime('%Y%m%d')

        # Payload Hash
        if body is None:
            payload_hash = hashlib.sha256(b'').hexdigest()
        elif isinstance(body, bytes):
            payload_hash = hashlib.sha256(body).hexdigest()
        elif isinstance(body, str):
            payload_hash = hashlib.sha256(body.encode('utf-8')).hexdigest()
        elif 'x-amz-content-sha256' in headers:
            payload_hash = headers['x-amz-content-sha256']
        elif hasattr(body, 'read') and hasattr(body, 'seek'):
            # For file-like objects, read to hash and seek back
            pos = body.tell()
            payload_hash = hashlib.sha256(body.read()).hexdigest()
            body.seek(pos)
        else:
            payload_hash = 'UNSIGNED-PAYLOAD' # Fallback

        # Canonical Headers
        # We MUST include the host header. urllib3 adds it at request time, 
        # but for signing we need to know what it will be.
        # We force 'host' into our headers dict so urllib3 uses it (or overrides, but the signed value matches).
        headers['host'] = host
        headers['x-amz-date'] = amz_date
        headers['x-amz-content-sha256'] = payload_hash

        # Sort headers for canonical string
        # AWS requires lowercase keys
        canonical_headers_list = []
        signed_headers_list = []
        for k, v in sorted(headers.items(), key=lambda x: x[0].lower()):
            k_lower = k.lower()
            # Skip 'authorization' if present (e.g. from previous attempt)
            if k_lower == 'authorization':
                continue
            # Sign x-amz-*, host, and content-length/type
            if k_lower == 'host' or k_lower.startswith('x-amz-') or k_lower in ('content-length', 'content-type'):
                v_trimmed = ' '.join(v.split()) # Remove extra whitespace
                canonical_headers_list.append(f"{k_lower}:{v_trimmed}")
                signed_headers_list.append(k_lower)
        
        canonical_headers = '\n'.join(canonical_headers_list) + '\n'
        signed_headers = ';'.join(signed_headers_list)

        # Canonical Request
        canonical_request = '\n'.join([
            request_method.upper(),
            path,
            canonical_query_string,
            canonical_headers,
            signed_headers,
            payload_hash
        ])

        # String to Sign
        algorithm = 'AWS4-HMAC-SHA256'
        credential_scope = f"{datestamp}/{self.region}/{self.service}/aws4_request"
        string_to_sign = '\n'.join([
            algorithm,
            amz_date,
            credential_scope,
            hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()
        ])

        # Calculate Signature
        signing_key = getSignatureKey(self.secret_key, datestamp, self.region, self.service)
        signature = hmac.new(signing_key, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()

        # Authorization Header
        authorization_header = (
            f"{algorithm} Credential={self.access_key}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        )
        
        headers['Authorization'] = authorization_header