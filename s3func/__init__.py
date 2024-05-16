"""Simple functions for working with S3"""
from s3func.main import s3_client, url_session, url_to_stream, base_url_to_stream, get_object, get_object_combo, put_object, list_objects, list_object_versions, delete_objects, put_object_legal_hold, put_object_lock_configuration, get_object_legal_hold, head_object, url_to_headers, base_url_to_headers
from s3func import utils

__version__ = '0.1.4'
