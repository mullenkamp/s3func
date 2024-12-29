"""Simple functions for working with S3"""
from s3func.s3 import S3Session, S3Lock
from s3func.b2 import B2Session, B2Lock
from s3func.http_url import HttpSession
from s3func import s3, http_url, utils, b2

__version__ = '0.7.0'
