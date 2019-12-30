import boto3
from boto3_type_annotations.s3 import Client
import logging


class S3Bucket:
    """
    A helper class for:
    - connecting to S3 buckets,
    - listing/filtering the contents,
    - adding/deleting object from the bucket
    """
    bucket_name: str
    _prefix: str
    _next_continuation_token: str
    _bucket_contents: dict
    __s3_client: Client = boto3.client('s3')

    def __init__(self, bucket_name: str, prefix: str = ''):
        self.__s3_client = boto3.client('s3')
        self.bucket_name = bucket_name
        self._prefix = prefix
        # check if we can connect successfully
        # throws botocore.exceptions.ClientError on error
        self.__s3_client.head_bucket(Bucket=self.bucket_name)

    @property
    def object_prefix(self):
        return self._prefix

    @object_prefix.setter
    def object_prefix(self, value):
        self._prefix = value
        self._next_continuation_token = None
        self._bucket_contents = {}

    @property
    def iter_objects(self):
        if self._bucket_contents != {}:
            for obj in self._bucket_contents:
                yield obj
            raise StopIteration

        bucket_objects = self._get_bucket_objects()
        while bucket_objects:
            for bucket_object in bucket_objects:
                yield bucket_object
            bucket_objects = self._get_bucket_objects()

    @property
    def all_objects(self):
        if self._bucket_contents != {}:
            return self._bucket_contents

        bucket_objects = self._get_bucket_objects()
        all_objects = {}
        while bucket_objects:
            all_objects.update(bucket_objects)
            bucket_objects = self._get_bucket_objects()

        self._bucket_contents = all_objects
        return self._bucket_contents

    def _get_bucket_objects(self):
        if self._next_continuation_token is not None:
            logging.debug('Getting a bucket [%s] continuation listing', self.bucket_name)
            resp = self.__s3_client.list_objects_v2(Bucket=self.bucket_name,
                                                    Prefix=self._prefix,
                                                    ContinuationToken=self._next_continuation_token)
        else:
            logging.debug('Getting a first bucket [%s] listing', self.bucket_name)
            resp = self.__s3_client.list_objects_v2(Bucket=self.bucket_name,
                                                    Prefix=self._prefix)

        if resp['IsTruncated']:
            self._next_continuation_token = resp['NextContinuationToken']
        else:
            self._next_continuation_token = None

        ret_dict = {x['Key']: {'mtime': x['LastModified'].replace(tzinfo=None), 'size': x['Size']}
                    for x in resp['Contents']}
        logging.info('Received bucket [%s] contents', self.bucket_name)
        return ret_dict

    def update_object(self, key: str, filename: str):
        self.__s3_client.upload_file(filename, self.bucket_name, key)

    def delete_object_in_s3(self, key: str):
        self.__s3_client.delete_object(Bucket=self.bucket_name, Key=key)

