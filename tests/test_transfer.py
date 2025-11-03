import unittest
from unittest.mock import Mock, patch

import pandas as pd

from tests.setup import setup
from to_data_library.data import transfer


class TestTransfer(unittest.TestCase):

    @patch('google.cloud.bigquery.Client')
    @patch("to_data_library.data.gs.storage.Client")
    def test_bq_to_gs(self, mock_storage, mock_bigquery):
        mock_bigquery_client = mock_bigquery.return_value
        mock_extract_job = Mock()
        mock_bigquery_client.extract_table.return_value = mock_extract_job
        mock_extract_job.result.return_value = ''

        mock_storage_client = mock_storage.return_value

        client = transfer.Client(project='fake_project')

        client.bq_to_gs(
            table='{}.{}.{}'.format('fake_project', 'fake_dataset_id', 'fake_table_id'),
            bucket_name='fake_bucket_name',
        )

        mock_storage_client.list_blobs.assert_called_once_with('fake_bucket_name')

    def test_build_gs_bucket_name(self):
        client = transfer.Client(project='fake_project')
        bucket_name = client.build_gs_bucket_name(
            business_type='fake_business_type',
            source_type='fake_sales_type'
        )
        expected_bucket_name = '-'.join(['fake_project', 'fake_business_type', 'fake_sales_type'])
        self.assertEqual(bucket_name, expected_bucket_name)

    def test_build_gs_prefix(self):
        client = transfer.Client(project='fake_project')
        prefix = client.build_gs_prefix(
            source='fake-source',
            ingestion_type='batch',
            etl_datetime_utc='2024-01-01T00:00:00Z',
            data_date='2024-01-01'
        )
        expected_prefix = 'fake-source/batch/2024-01-01/2024-01-01T00:00:00Z'
        self.assertEqual(prefix, expected_prefix)

    def test_build_gs_file_name(self):
        client = transfer.Client(project='fake_project')
        file_name = client.build_gs_file_name(
            source='fake-source',
            dimension='fake-dimension',
            etl_datetime_utc='2024-01-01T00:00:00Z',
            data_date='2024-01-01',
            file_number='001',
            file_extension='csv'
        )
        expected_file_name = 'fake-source_fake-dimension_2024-01-01_2024-01-01T00:00:00Z_001.csv'
        self.assertEqual(file_name, expected_file_name)

    @patch('to_data_library.data.gs.Client')
    @patch('to_data_library.data.bq.Client')
    def test_gs_to_bq(self, mock_bq_client, mock_gs_client):

        mock_prefix = 'fake-source/batch/2024-01-01/2024-01-01T00:00:00Z'
        mock_file_name = 'fake-source_fake-dimension_2024-01-01_2024-01-01T00:00:00Z_001.csv'
        mock_csv_content = b"name,number,date\nabc,1,'2024-01-01'\ndef,2,'2024-01-01'"

        # Mock get_blobs on the gs.Client
        mock_blob = Mock()
        mock_blob.name = mock_prefix + '/' + mock_file_name
        mock_blob.download_as_bytes.return_value = mock_csv_content
        mock_gs_client.return_value.get_blobs = Mock(return_value=[mock_blob])

        # Mock load_table_from_dataframe on the bq.Client
        mock_bq_client.return_value.load_table_from_dataframe = Mock()

        client = transfer.Client('fake_project_name')

        def transform_function(df):
            df["date"] = pd.to_datetime(df["date"]).dt.date
            return df

        client.gs_to_bq(
            business_type='fake_business_type',
            source_type='fake_source_type',
            source='fake-source',
            ingestion_type='batch',
            etl_datetime_utc='2024-01-01T00:00:00Z',
            dimension='fake-dimension',
            table='{}.{}.{}'.format('fake_project_name', 'fake_dataset_id', 'fake_table_id'),
            file_number='001',
            write_preference='truncate',
            max_bad_records=10,
            schema_update_options=['ALLOW_FIELD_ADDITION'],
            partition_field='date',
            data_date='2024-01-01',
            transform_function=transform_function
        )

        print(mock_bq_client.return_value.load_table_from_dataframe.call_args)
        # Assert load_table_from_dataframe is called with the right parameters

        expected_df = transform_function(pd.DataFrame({
                'name': ['abc', 'def'],
                'number': [1, 2],
                'date': ['2024-01-01', '2024-01-01'],
                'etl_datetime_utc': ['2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z']
            }))

        # Check load_table_from_dataframe called once
        mock_bq_client.return_value.load_table_from_dataframe.assert_called_once()

        actual_args, actual_kwargs = mock_bq_client.return_value.load_table_from_dataframe.call_args
        actual_df = actual_args[0]  # Adjust index if DataFrame is not the first argument

        # Check load_table_from_dataframe called with expected DataFrame
        pd.testing.assert_frame_equal(actual_df, expected_df)

        # Check the rest of the arguments used in load_table_from_dataframe:
        assert actual_args[1:] == (
            'fake_project_name.fake_dataset_id.fake_table_id',
            'truncate',
            True,
            None,
            '20240101',
            'date',
            {'max_bad_records': 10, 'schema_update_options': ['ALLOW_FIELD_ADDITION']}
        )

    @patch('boto3.client')
    @patch('boto3.resource')
    @patch('to_data_library.data.s3.Client')
    @patch("to_data_library.data.gs.storage.Client")
    def test_s3_to_gs(self, mock_storage, mock_s3_client, mock_s3_resource, mock_s3_boto):
        mock_aws_session = Mock()
        mock_aws_session.return_value = 'fake_session'
        mock_aws_session.client.return_value.get_paginator().paginate.return_value = []

        client = transfer.Client(project='fake_project')

        client.s3_to_gs(
            aws_session=mock_aws_session,
            s3_bucket_name='fake_s3_bucket',
            s3_object_or_prefix_name='download_sample.csv',
            business_type='fake_business_type',
            source_type='fake_source_type',
            source='fake-source',
            dimension='fake-dimension',
            repo_name='fake_repo_name',
            ingestion_type='batch',
            file_number='001',
            wildcard=None,
            data_date='2024-01-01',
            etl_datetime_utc='2024-01-01T00:00:00Z'
        )

        mock_s3_client.assert_called_once()

    def test_get_keys_in_s3_bucket(self):
        mock_aws_session = Mock()
        mock_aws_session.client.return_value.get_paginator().paginate.return_value = []

        client = transfer.Client(project=setup.project)
        res = client._get_keys_in_s3_bucket(mock_aws_session, 'fake_bucket_name', 'fake_prefix_name')

        self.assertEqual(res, [])

    def test_load_gcs_file_as_dataframe_csv(self):
        mock_blob = Mock()
        mock_blob.name = 'test.csv'
        mock_blob.download_as_bytes.return_value = b'a,b\n1,2\n3,4'
        client = transfer.Client(project='fake_project')
        df = client.load_gcs_file_as_dataframe(mock_blob)
        expected_df = pd.DataFrame({'a': [1, 3], 'b': [2, 4]})
        pd.testing.assert_frame_equal(df, expected_df)

    def test_load_gcs_file_as_dataframe_json(self):
        mock_blob = Mock()
        mock_blob.name = 'test.json'
        mock_blob.download_as_bytes.return_value = b'{"a": 1, "b": 2}\n{"a": 3, "b": 4}'
        client = transfer.Client(project='fake_project')
        df = client.load_gcs_file_as_dataframe(mock_blob)
        expected_df = pd.DataFrame({'a': [1, 3], 'b': [2, 4]})
        pd.testing.assert_frame_equal(df, expected_df)

    def test_load_gcs_file_as_dataframe_unsupported(self):
        mock_blob = Mock()
        mock_blob.name = 'test.txt'
        mock_blob.download_as_bytes.return_value = b''
        client = transfer.Client(project='fake_project')
        with self.assertRaises(ValueError) as exc:
            client.load_gcs_file_as_dataframe(mock_blob)
        self.assertIn('Unsupported file type', str(exc.exception))


class TestTransferEdgeCases(unittest.TestCase):
    def setUp(self):
        self.client = transfer.Client('fake_project')

    @patch('to_data_library.data.gs.Client')
    @patch('to_data_library.data.bq.Client')
    @patch.object(transfer.logs.client.logger, 'error')
    def test_gs_to_bq_invalid_source_format(self, mock_error, mock_bq_client, mock_gs_client):
        mock_prefix = 'fake-source/batch/2024-01-01T00:00:00Z'
        mock_file_name = 'fake-source_fake-dimension_2024-01-01T00:00:00Z_001.not_a_file_type'

        # Mock get_blobs on the gs.Client
        mock_blob = Mock()
        mock_blob.name = mock_prefix + '/' + mock_file_name
        mock_blob.download_as_bytes.return_value = b"some content"
        mock_gs_client.return_value.get_blobs = Mock(return_value=[mock_blob])

        success, message = self.client.gs_to_bq(
                business_type='fake_business_type',
                source_type='fake_source_type',
                source='fake-source',
                ingestion_type='batch',
                etl_datetime_utc='2024-01-01T00:00:00Z',
                dimension='fake-dimension',
                table='{}.{}.{}'.format('fake_project_name', 'fake_dataset_id', 'fake_table_id')
        )
        assert not success
        assert message == 'Unsupported file type: not_a_file_type'

    @patch('to_data_library.data.s3.Client')
    @patch('to_data_library.data.gs.Client')
    @patch('os.remove')
    @patch('os.path.exists', return_value=True)
    @patch.object(transfer.logs.client.logger, 'info')
    def test_s3_to_gs_cleanup_and_logging(self, mock_info, mock_path_exists, mock_remove, mock_gs, mock_s3):
        mock_s3.return_value.download.return_value = '/tmp/file.csv'
        mock_gs.return_value.upload.return_value = None
        # Simulate one file found
        with patch.object(self.client, '_get_keys_in_s3_bucket', return_value=['file.csv']):

            self.client.s3_to_gs(
                aws_session=Mock(),
                s3_bucket_name='fake_s3_bucket',
                s3_object_or_prefix_name='file.csv',
                business_type='fake_business_type',
                source_type='fake_source_type',
                source='fake-source',
                dimension='fake-dimension'
            )

            mock_remove.assert_called_once_with('/tmp/file.csv')
            mock_info.assert_any_call('Deleted local file /tmp/file.csv')

    def test_get_keys_in_s3_bucket_empty_and_missing_contents(self):
        # Empty pages
        mock_aws_session = Mock()
        mock_aws_session.client.return_value.get_paginator.return_value.paginate.return_value = []
        res = self.client._get_keys_in_s3_bucket(mock_aws_session, 'bucket', 'prefix')
        self.assertEqual(res, [])
        # Page with no 'Contents'
        mock_aws_session.client.return_value.get_paginator.return_value.paginate.return_value = [{'NoContents': True}]
        with patch('builtins.print'):
            res = self.client._get_keys_in_s3_bucket(mock_aws_session, 'bucket', 'prefix')
        self.assertEqual(res, [])
