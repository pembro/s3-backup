import pathlib
import datetime
import configparser
import logging
import boto3
from boto3_type_annotations.s3 import Client
from typing import List


class SsbBucketFolder:
    bucket_target: str
    bucket_folder: str
    source_folder: pathlib.Path
    deletes: bool
    __s3: Client = boto3.client('s3')

    def __init__(self, d: configparser.ConfigParser, config_path: pathlib.Path):
        self.bucket_target = d.get('bucket-target')
        self.bucket_folder = d.get('bucket-folder', fallback='')
        if self.bucket_folder != '':
            self.bucket_folder += '/'
        self.source_folder = config_path / pathlib.Path(d.get('source'))
        self.source_folder.resolve()
        self.deletes = d.getboolean('sync-deletes')
        self.__s3 = boto3.client('s3')
        logging.debug('Created a new bucket (bucketTarget=%s, bucketFolder=%s, sourceFolder=%s ,deletes=%s',
                      self.bucket_target, self.bucket_target, self.source_folder, (self.deletes))

    def update_object_in_s3(self, local_path: str):
        key_to_update = self.build_bucket_key(local_path)
        local_file = self.source_folder / local_path
        local_file = str(local_file.resolve())
        logging.info(f'Uploading [%s] to bucket key [%s]', local_file, key_to_update)
        ret = self.__s3.upload_file(local_file, self.bucket_target, key_to_update)
        return ret

    def delete_object_in_s3(self, local_path: str):
        key_to_delete = self.build_bucket_key(local_path)
        logging.info(f'Deleting bucket object [%s]', key_to_delete)
        self.__s3.delete_object(Bucket=self.bucket_target, Key=key_to_delete)

    def build_bucket_key(self, local_name: str):
        return self.bucket_folder + local_name

    def sync_folder(self):
        local_objects = self.get_local_objects()
        bucket_objects = self.get_bucket_objects()
        print(local_objects)
        print(bucket_objects)
        self.update_bucket_objects(local_objects, bucket_objects)

    def update_bucket_objects(self, local: dict, remote: dict):
        # objects to delete first
        if self.deletes:
            for remote_key in remote.keys():
                if remote_key not in local.keys():
                    self.delete_object_in_s3(remote_key)

        for local_key in local.keys():
            if local_key not in remote.keys():
                self.update_object_in_s3(local_key)
            else:
                cond_newer_file = local[local_key]['mtime'] > remote[remote_key]['mtime']
                cond_diff_size = local[local_key]['size'] != remote[remote_key]['size']
                if cond_newer_file and cond_diff_size:
                    logging.debug('Local object [%s] differs from remote', local_key)
                    self.update_object_in_s3(local_key)

    def get_local_objects(self) -> dict:
        str_source = self.source_folder.as_posix()
        source_files = {}

        for source_file in self.source_folder.rglob("*"):
            if source_file.is_dir():
                continue
            str_source_file = source_file.as_posix()
            str_source_file = str_source_file.replace(f'{str_source}/', "")
            source_mtime = datetime.datetime.utcfromtimestamp(source_file.stat().st_mtime // 1)
            source_size = source_file.stat().st_size
            source_files[str_source_file] = {'mtime': source_mtime, 'size': source_size}

        return source_files

    def get_bucket_objects(self) -> dict:
        try:
            bucket_resp = self.__s3.list_objects_v2(Bucket=self.bucket_target, Prefix=self.bucket_folder)
            bucket_contents = {x['Key'].replace(f'{self.bucket_folder}', ''):
                                   {'mtime': x['LastModified'].replace(tzinfo=None), 'size': x['Size']}
                               for x in bucket_resp['Contents']}
            logging.info('Received bucket contents')
        except KeyError:
            logging.warning('No objects in the bucket folder')
            return {}
        else:
            return bucket_contents


class SsbConfig:
    """
    This class finds if we are running in a directory that is configured
    for syncing with S3. The config file could be either in the current
    directory or one of the parent directories (similar to a git repo).

    """
    sync_folders: List[SsbBucketFolder]

    def _validate_config(self, config: configparser.ConfigParser, config_path: pathlib.Path):
        for syn_dir in config['dirs']:
            logging.debug('Processing config entry %s', syn_dir)
            new_dir = SsbBucketFolder(config[syn_dir], config_path)
            self.sync_folders.append(new_dir)

    def __init__(self, filename: str = 'ssbconfig.ini'):
        path = pathlib.Path().resolve()
        self.sync_folders = []
        config_file_found = False

        # Iterate over the path up the folder tree until the config file is found
        # Fails if no config is found
        while not path.samefile(path.root):
            config_file = path / filename
            if config_file.is_file():
                logging.info('Config file found at %s', config_file.resolve())
                config = configparser.ConfigParser()
                config.read(config_file)
                self._validate_config(config, path)
                config_file_found = True
                break
            path = path.parent

        if config_file_found is False:
            logging.error("Config file not found. Are you sure this is a sync directory?")
            exit(1)

    def sync_with_s3(self):
        for sync_folder in self.sync_folders:
            sync_folder.sync_folder()
