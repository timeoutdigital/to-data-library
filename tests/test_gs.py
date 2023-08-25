import unittest
import os

from to_data_library.data.gs import Client
from tests.setup import setup
from google.cloud import storage


def setUpModule():
    setup.create_bucket()


def tearDownModule():
    setup.delete_bucket()
    setup.cleanup()


class TestGS(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestGS, self).__init__(*args, **kwargs)
        self.setup = setup
        self.gs_uri = f'gs://{self.setup.bucket_name}/sample.csv'
        
    def test_download(self):
        test_client = Client(project=self.setup.project)
        
        test_client.download(gs_uri=self.gs_uri)
        self.assertTrue(os.path.exists('sample.csv'))
        
        test_client.download(gs_uri=self.gs_uri,
                             destination_file_name='download_sample.csv')
        self.assertTrue(os.path.exists('download_sample.csv'))

    def test_upload(self):
        test_client = Client(project=self.setup.project)
        test_client.upload('tests/data/sample.csv', self.setup.bucket_name, 'test_file.csv')
        
        storage_client = storage.Client()
        blob_list = [blob.name for blob in storage_client.list_blobs(self.setup.bucket_name)]
    
        self.assertIn('test_file.csv', blob_list)
        
        
    def test_create_bucket(self):
        test_bucket_name = '1234_sample_bucket'
        test_client = Client(project=self.setup.project)
        test_client.create_bucket(test_bucket_name)
        
        storage_client = storage.Client(project=self.setup.project)
        test_bucket = storage_client.get_bucket(test_bucket_name)
        bucket_list = [bucket.name for bucket in storage_client.list_buckets()]
        
        self.assertIn(test_bucket_name, bucket_list)
        test_bucket.delete()
        
    @unittest.expectedFailure
    def test_create_bucket_already_exists(self):
        # Expected failure as this bucket is created during setUp.
        test_client = Client(project=self.setup.project)
        test_client.create_bucket(self.setup.bucket_name)
