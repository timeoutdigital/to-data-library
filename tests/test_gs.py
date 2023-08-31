import os
import unittest
from unittest import mock
from unittest.mock import Mock

from google.cloud import storage

from tests.setup import setup
from to_data_library.data.gs import Client

# def setUpModule():
#     setup.create_bucket()


def tearDownModule():
    setup.cleanup()


class TestGS(unittest.TestCase):

    # def __init__(self, *args, **kwargs):
    #     super(TestGS, self).__init__(*args, **kwargs)
    #     self.setup = setup
    #     self.gs_uri = f'gs://{self.setup.bucket_name}/sample.csv'

    @mock.patch('to_data_library.data.gs.storage')
    def test_download(self, mock_storage):
        mock_gcs_client = mock_storage.Client.return_value
        mock_bucket = Mock()
        mock_gcs_client.bucket.return_value = mock_bucket
        mock_gcs_client.download_blob_to_file.return_value = None

        test_client = Client(project='fake_project')

        test_client.download(gs_uri='/fake_uri.csv')
        self.assertTrue(os.path.exists('fake_uri.csv'))

        test_client.download(gs_uri='/fake_uri', destination_file_name='fake_des.csv')
        self.assertTrue(os.path.exists('fake_des.csv'))

    @mock.patch('to_data_library.data.gs.storage')
    def test_upload(self, mock_storage):
        mock_gcs_client = mock_storage.Client.return_value
        mock_bucket = Mock()
        mock_bucket.blob.return_value = 'test_file.csv'
        mock_gcs_client.bucket.return_value = mock_bucket

        test_client = Client(project='fake_project')
        test_client.upload('tests/data/sample.csv', self.setup.bucket_name, 'test_file.csv')

        storage_client = storage.Client()
        blob_list = [blob.name for blob in storage_client.list_blobs(self.setup.bucket_name)]

        self.assertIn('test_file.csv', blob_list)

    @mock.patch('to_data_library.data.gs.storage')
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
