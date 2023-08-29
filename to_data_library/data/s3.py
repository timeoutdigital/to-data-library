import os
import sys

import boto3
import botocore
import parse

from to_data_library.data import logs


class Client:
    """
        Client to s3 Storage functionality.

        Args:
            connection_string (str): The S3 connection string in the format
                                     {region}:{access_key}:{secret_key}
    """

    def __init__(self, connection_string):
        parsed_connection = parse.parse('{region}:{access_key}:{secret_key}', connection_string)
        self.s3_client = boto3.resource(service_name='s3',
                                        region_name=parsed_connection['region'],
                                        aws_access_key_id=parsed_connection['access_key'],
                                        aws_secret_access_key=parsed_connection['secret_key'])

    def download(self, bucket_name, object_name, local_path='.'):
        """
        Downloads a file from the s3 to the local system.

        Args:
            bucket_name (str): s3 bucket name
            object_name (str): s3 file name to download
            local_path (str): local file name with path. If local path is a directory
                              object name is used as local file name

        Example:
            >>> from to_data_library.data import s3
            >>> client = s3.Client('region:access_key:secret_key')
            >>> client.download(bucket_name='my-s3-bucket-name',
            >>>                 object_name='folder-name/object-name',
            >>>                 local_path='/my-local/folder/file.csv')
        """
        if os.path.isdir(local_path):
            filename = os.path.basename(object_name)
            local_path = os.path.join(local_path, filename)

        try:
            logs.client.logger.info(f"Downloading {object_name} from {bucket_name} s3 bucket")
            bucket = self.s3_client.Bucket(bucket_name)
            bucket.download_file(object_name, local_path)
        except botocore.exceptions.ClientError as e:
            logs.client.logger.error(e)
            sys.exit(1)

        logs.client.logger.info("File download completed")

    def upload(self, local_path, bucket_name, object_name=None):
        """
        Uploads a local file to s3 bucket

        Args:
            local_path (str): File with path to upload
            bucket_name (str): s3 bucket name
            object_name (str): S3 object name. If not specified then local file_name is used

        Example:
            >>> from to_data_library.data import s3
            >>> client = s3.Client('region:access_key:secret_key')
            >>> client.upload(local_path='/my-local/folder/file.csv',
            >>>               bucket_name='my-s3-bucket-name',
            >>>               object_name='object-name')
        """

        if object_name is None:
            object_name = os.path.basename(local_path)

        logs.client.logger.info(f"Uploading {local_path} to {bucket_name}/{object_name} s3 bucket")
        bucket = self.s3_client.Bucket(bucket_name)
        bucket.upload_file(local_path, object_name)
        logs.client.logger.info("File upload completed")

    def list_files(self, bucket_name, path=None):
        """Lists the files in the s3 bucket

        Args:
            bucket_name (str): s3 bucket name
            path (str): s3 bucket sub folder

        Returns:
            list: The list of the files

        Example:
            >>> from to_data_library.data import s3
            >>> client = s3.Client('region:access_key:secret_key')
            >>> client.list_files(bucket_name='s3-bucket-name', path='/path/inside/bucket/')
        """

        bucket = self.s3_client.Bucket(bucket_name)
        logs.client.logger.info("Listing files from {}/{} s3 bucket".format(bucket_name, path))
        if path:
            objects = bucket.objects.filter(Prefix=path)
            files = [{'name': object.key, 'last_modified': object.last_modified}
                     for object in objects if object.key != path]
        else:
            objects = bucket.objects.all()
            files = [{'name': object.key, 'last_modified': object.last_modified} for object in objects]

        if files:
            logs.client.logger.info("Files: {}".format(', '.join([file['name'] for file in files])))
        else:
            logs.client.logger.info("Files: No files found")

        return files
