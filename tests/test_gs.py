import unittest
from unittest import mock
from unittest.mock import Mock

from tests.setup import setup
from to_data_library.data.gs import Client


def tearDownModule():
    setup.cleanup()


class TestGS(unittest.TestCase):
    @mock.patch('to_data_library.data.gs.storage.Client')
    def test_download(self, mock_storage_client):
        mock_client_instance = mock_storage_client.return_value
        test_client = Client(project='fake_project')
        mock_client_instance.download_blob_to_file = mock.Mock()

        gs_uri = 'gs://fake_bucket/fake_file.csv'

        test_client.download(gs_uri)

        mock_client_instance.download_blob_to_file.assert_called_once()
        called_args = mock_client_instance.download_blob_to_file.call_args[0]
        assert called_args[0] == gs_uri, f"Expected gs_uri {gs_uri}, got {called_args[0]}"
        assert hasattr(called_args[1], 'write'), "Expected a file-like object"

    @mock.patch('to_data_library.data.gs.storage')
    def test_upload(self, mock_storage):
        mock_client = mock_storage.Client.return_value
        mock_bucket = Mock()
        mock_client.bucket.return_value = mock_bucket

        test_client = Client(project='fake_project')

        test_client.upload('tests/data/sample.csv', 'fake_bucket')
        mock_bucket.blob.assert_called_with('sample.csv')

        test_client.upload('tests/data/sample.csv', 'fake_bucket', 'new_name')
        mock_bucket.blob.assert_called_with('new_name')
