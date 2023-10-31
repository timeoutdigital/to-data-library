import os
import unittest

import boto3
from moto import mock_s3
from unittest import mock
from unittest.mock import Mock

from tests.setup import setup
from to_data_library.data.s3 import Client


def tearDownModule():
    setup.cleanup()


@mock_s3
class TestS3(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestS3, self).__init__(*args, **kwargs)
        self.setup = setup

    def setUp(self):
        s3 = boto3.resource("s3", region_name=self.setup.s3_region)
        bucket = s3.Bucket(self.setup.s3_bucket)
        bucket.create(CreateBucketConfiguration={
                'LocationConstraint': self.setup.s3_region,
            },
        )
        content = b"dummy-content"
        key = 'download_sample.csv'
        object = s3.Object(self.setup.s3_bucket, key)
        object.put(Body=content)

    def test_download(self):
        mock_aws_session = boto3.Session()
        test_client = Client(mock_aws_session)

        test_client.download(self.setup.s3_bucket,
                             'download_sample.csv')
        self.assertTrue(os.path.exists('download_sample.csv'))

        test_client.download(self.setup.s3_bucket,
                             'download_sample.csv',
                             'download_s3_sample.csv')

        self.assertTrue(os.path.exists('download_s3_sample.csv'))

    def test_upload(self):
        mock_aws_session = boto3.Session()
        test_client = Client(mock_aws_session)

        test_client.upload('tests/data/sample.csv',
                           self.setup.s3_bucket)
        s3_client = boto3.resource(
            service_name='s3',
            region_name=self.setup.s3_region
        )
        bucket = s3_client.Bucket(setup.s3_bucket)
        obj = list(bucket.objects.filter(Prefix='sample.csv'))
        self.assertTrue(any(w.key == 'sample.csv' for w in obj))

        test_client.upload('tests/data/sample.csv',
                           setup.s3_bucket,
                           's3_upload_file.csv')
        s3_client = boto3.resource(
            service_name='s3',
            region_name=self.setup.s3_region
        )
        bucket = s3_client.Bucket(setup.s3_bucket)
        obj = list(bucket.objects.filter(Prefix='s3_upload_file.csv'))
        self.assertTrue(any(w.key == 's3_upload_file.csv' for w in obj))
