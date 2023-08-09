import unittest
import os
import boto3

from to_data_library.data.s3 import Client
from tests.setup import setup


def setUpModule():
    setup.upload_s3_files()


def tearDownModule():
    setup.remove_s3_files()
    setup.cleanup()


class TestS3(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestS3, self).__init__(*args, **kwargs)
        self.setup = setup

    def test_download(self):
        test_client = Client(f'{self.setup.s3_region}:{self.setup.s3_access_key}:{self.setup.s3_secret_key}')

        test_client.download(setup.s3_bucket,
                             'download_sample.csv')
        self.assertTrue(os.path.exists('download_sample.csv'))

        test_client.download(setup.s3_bucket,
                             'download_sample.csv',
                             'download_s3_sample.csv')

        self.assertTrue(os.path.exists('download_s3_sample.csv'))

    def test_upload(self):
        test_client = Client(f'{self.setup.s3_region}:{self.setup.s3_access_key}:{self.setup.s3_secret_key}')

        test_client.upload('tests/data/sample.csv',
                           setup.s3_bucket)
        s3_client = boto3.resource(service_name='s3',
                                   region_name=self.setup.s3_region,
                                   aws_access_key_id=self.setup.s3_access_key,
                                   aws_secret_access_key=self.setup.s3_secret_key)
        bucket = s3_client.Bucket(setup.s3_bucket)
        obj = list(bucket.objects.filter(Prefix='sample.csv'))
        self.assertTrue(any([w.key == 'sample.csv' for w in obj]))

        test_client.upload('tests/data/sample.csv',
                           setup.s3_bucket,
                           's3_upload_file.csv')
        s3_client = boto3.resource(service_name='s3',
                                   region_name=self.setup.s3_region,
                                   aws_access_key_id=self.setup.s3_access_key,
                                   aws_secret_access_key=self.setup.s3_secret_key)
        bucket = s3_client.Bucket(setup.s3_bucket)
        obj = list(bucket.objects.filter(Prefix='s3_upload_file.csv'))
        self.assertTrue(any([w.key == 's3_upload_file.csv' for w in obj]))
