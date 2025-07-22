import gzip
import json
import unittest
from io import BytesIO
from unittest import mock
from unittest.mock import MagicMock, Mock

from tests.setup import setup
from to_data_library.data.gs import Client


def tearDownModule():
    setup.cleanup()


def gzip_bytes_from_str(s):
    buf = BytesIO()
    with gzip.GzipFile(fileobj=buf, mode='wb') as f:
        f.write(s.encode('utf-8'))
    buf.seek(0)
    return buf.getvalue()


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
        self.assertEqual(called_args[0], gs_uri)
        self.assertTrue(hasattr(called_args[1], 'write'))

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

    @mock.patch('to_data_library.data.gs.storage')
    def test_convert_json_array_to_ndjson_array_input(self, mock_storage):
        mock_client = mock_storage.Client.return_value
        mock_bucket = MagicMock()
        mock_client.bucket.return_value = mock_bucket

        input_data = json.dumps([{"a": 1}, {"b": 2}])
        gz_bytes = gzip_bytes_from_str(input_data)

        input_blob = Mock()
        input_blob.download_as_bytes.return_value = gz_bytes

        output_blob = Mock()
        output_blob.exists.return_value = False

        mock_bucket.blob.side_effect = [input_blob, output_blob]

        test_client = Client(project='fake_project')
        test_client.storage_client = mock_client
        test_client.logger = Mock()

        test_client.convert_json_array_to_ndjson(
            bucket_name='fake_bucket',
            input_gz_file='input.gz',
            output_file='output.ndjson'
        )

        args, kwargs = output_blob.upload_from_file.call_args
        uploaded_content = args[0].read().decode()
        self.assertIn('{"a": 1}', uploaded_content)
        self.assertIn('{"b": 2}', uploaded_content)

    @mock.patch('to_data_library.data.gs.storage')
    def test_convert_json_array_to_ndjson_ndjson_input(self, mock_storage):
        mock_client = mock_storage.Client.return_value
        mock_bucket = MagicMock()
        mock_client.bucket.return_value = mock_bucket

        ndjson_str = '{"x": 10}\n{"y": 20}\n'
        gz_bytes = gzip_bytes_from_str(ndjson_str)

        input_blob = Mock()
        input_blob.download_as_bytes.return_value = gz_bytes

        output_blob = Mock()
        output_blob.exists.return_value = False

        mock_bucket.blob.side_effect = [input_blob, output_blob]

        test_client = Client(project='fake_project')
        test_client.storage_client = mock_client
        test_client.logger = Mock()

        test_client.convert_json_array_to_ndjson(
            bucket_name='fake_bucket',
            input_gz_file='ndjson_input.gz',
            output_file='ndjson_output.ndjson'
        )

        args, kwargs = output_blob.upload_from_file.call_args
        uploaded = args[0].read().decode()
        self.assertIn('{"x": 10}', uploaded)
        self.assertIn('{"y": 20}', uploaded)

    @mock.patch('to_data_library.data.gs.storage')
    def test_convert_json_array_to_ndjson_malformed_line(self, mock_storage):
        mock_client = mock_storage.Client.return_value
        mock_bucket = MagicMock()
        mock_client.bucket.return_value = mock_bucket

        bad_data = '{"valid": 1}\n{bad_json}\n{"also": "valid"}\n'
        gz_bytes = gzip_bytes_from_str(bad_data)

        input_blob = Mock()
        input_blob.download_as_bytes.return_value = gz_bytes

        output_blob = Mock()
        output_blob.exists.return_value = False

        mock_bucket.blob.side_effect = [input_blob, output_blob]

        test_client = Client(project='fake_project')
        test_client.storage_client = mock_client
        test_client.logger = Mock()

        test_client.convert_json_array_to_ndjson(
            bucket_name='fake_bucket',
            input_gz_file='bad_file.gz',
            output_file='cleaned.ndjson'
        )

        args, kwargs = output_blob.upload_from_file.call_args
        result = args[0].read().decode()
        self.assertIn('{"valid": 1}', result)
        self.assertIn('{"also": "valid"}', result)
        self.assertNotIn('bad_json', result)
        test_client.logger.warning.assert_called()

    @mock.patch('to_data_library.data.gs.storage')
    def test_convert_json_array_to_ndjson_overwrites_existing(self, mock_storage):
        mock_client = mock_storage.Client.return_value
        mock_bucket = MagicMock()
        mock_client.bucket.return_value = mock_bucket

        input_blob = Mock()
        input_blob.download_as_bytes.return_value = gzip_bytes_from_str('[{"a": "b"}]')

        output_blob = Mock()
        output_blob.exists.return_value = True  # Simulate pre-existing output

        mock_bucket.blob.side_effect = [input_blob, output_blob]

        test_client = Client(project='fake_project')
        test_client.storage_client = mock_client
        test_client.logger = Mock()

        test_client.convert_json_array_to_ndjson(
            bucket_name='fake_bucket',
            input_gz_file='input.gz',
            output_file='output.ndjson'
        )

        output_blob.delete.assert_called_once()
        output_blob.upload_from_file.assert_called()
