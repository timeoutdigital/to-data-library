import os
import unittest
from unittest import mock
from unittest.mock import Mock

from tests.setup import setup
from to_data_library.data.gs import Client


def tearDownModule():
    setup.cleanup()


class TestGS(unittest.TestCase):
    @mock.patch('to_data_library.data.gs.storage')
    def test_download(self, mock_storage):
        test_client = Client(project='fake_project')

        test_client.download(gs_uri='/fake_uri.csv')
        self.assertTrue(os.path.exists('fake_uri.csv'))

        test_client.download(gs_uri='/fake_uri', destination_file_name='fake_des.csv')
        self.assertTrue(os.path.exists('fake_des.csv'))

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
