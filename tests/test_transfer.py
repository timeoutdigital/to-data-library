import unittest
import unittest.mock
from unittest.mock import ANY, Mock, patch

from tests.setup import setup
from to_data_library.data import transfer


class TestTransfer(unittest.TestCase):
    @patch('google.cloud.bigquery.Client')
    @patch('google.cloud.storage.Client')
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

    @patch('google.cloud.bigquery.DatasetReference')
    @patch('google.cloud.bigquery.TableReference')
    @patch('to_data_library.data.bq.Client')
    @patch('google.cloud.bigquery.LoadJobConfig')
    @patch('to_data_library.data.bq.default')
    def test_gs_to_bq(self, mock_default, mock_loadjobconfig,
                     mock_bq_client, mock_tablereference, mock_datasetreference):

        mock_default.return_value = 'first', 'second'

        # Mock return values for LoadJobConfig
        mock_loadjobconfig.return_value.source_format = 'CSV'
        mock_loadjobconfig.return_value.skip_leading_rows = 1
        mock_loadjobconfig.return_value.autodetect = True
        mock_loadjobconfig.return_value.field_delimiter = ','
        mock_loadjobconfig.return_value.write_disposition = 'WRITE_TRUNCATE'
        mock_loadjobconfig.return_value.allow_quoted_newlines = True
        mock_loadjobconfig.return_value.max_bad_records = 10

        # Mock load_table_from_uris on the bq.Client
        mock_bq_client.return_value.load_table_from_uris = Mock()

        client = transfer.Client('fake_project_name')
        client.gs_to_bq(
            gs_uris="gs://{}/{}".format('fake_bucket_name', 'sample.csv'),
            table='{}.{}.{}'.format('fake_project_name', 'fake_dataset_id', 'fake_table_id'),
            write_preference='truncate',
            max_bad_records=10
        )

        mock_datasetreference.assert_called_with(project='fake_project_name', dataset_id='fake_dataset_id')
        mock_tablereference.assert_called_with(ANY, table_id='fake_table_id')

        # Assert that LoadJobConfig was created
        mock_loadjobconfig.assert_called_once()

        # Check that the job_config attributes are correct
        job_config = mock_loadjobconfig.return_value
        self.assertEqual(job_config.source_format, 'CSV')
        self.assertEqual(job_config.skip_leading_rows, 1)
        self.assertEqual(job_config.autodetect, True)
        self.assertEqual(job_config.field_delimiter, ',')
        self.assertEqual(job_config.write_disposition, 'WRITE_TRUNCATE')
        self.assertEqual(job_config.allow_quoted_newlines, True)
        self.assertEqual(job_config.max_bad_records, 10)

        # Assert load_table_from_uris is called with the right parameters
        mock_bq_client.return_value.load_table_from_uris.assert_called_once_with(
            "gs://fake_bucket_name/sample.csv",
            ANY,  # table_ref
            job_config=job_config
        )

    @patch('boto3.client')
    @patch('boto3.resource')
    @patch('to_data_library.data.bq.default')
    @patch('to_data_library.data.s3.Client')
    @patch('google.cloud.storage.Client')
    def test_s3_to_gs(self, mock_storage, mock_s3_client, mock_default, mock_s3_resource, mock_s3_boto):
        mock_aws_session = Mock()
        mock_aws_session.return_value = 'fake_session'
        mock_aws_session.client.return_value.get_paginator().paginate.return_value = []

        mock_default.return_value = 'first', 'second'
        client = transfer.Client(project=setup.project)
        client.s3_to_gs(aws_session=mock_aws_session,
                        s3_bucket_name='fake_s3_bucket',
                        s3_object_name='download_sample.csv',
                        gs_bucket_name='fake_gs_bucket_name',
                        gs_file_name='transfer_s3_to_gs.csv')

        mock_s3_client.assert_called_once()

    def test_get_keys_in_s3_bucket(self):
        mock_aws_session = Mock()
        mock_aws_session.client.return_value.get_paginator().paginate.return_value = []

        client = transfer.Client(project=setup.project)
        res = client._get_keys_in_s3_bucket(mock_aws_session, 'fake_bucket_name', 'fake_prefix_name')

        self.assertEqual(res, [])

    def test_gs_to_bq_schema_validation(self):
        client = transfer.Client('fake_project_name')
        valid_schema = [('field1', 'STRING'), ('field2', 'INTEGER')]
        with patch('google.cloud.bigquery.Client'), \
             patch('google.cloud.bigquery.DatasetReference'), \
             patch('google.cloud.bigquery.TableReference'), \
             patch('google.cloud.bigquery.LoadJobConfig'):
            # Should not raise
            client.gs_to_bq(
                gs_uris="gs://bucket/file.csv",
                table="p.d.t",
                write_preference="truncate",
                schema=valid_schema
            )
            # Invalid schema: wrong type
            with self.assertRaises(ValueError):
                client.gs_to_bq(
                    gs_uris="gs://bucket/file.csv",
                    table="p.d.t",
                    write_preference="truncate",
                    schema=[('field1', 'NOTATYPE')]
                )

    def test_ftp_to_bq_schema_validation(self):
        client = transfer.Client('fake_project_name')
        valid_schema = [('field1', 'STRING')]
        with patch('to_data_library.data.ftp.Client') as mock_ftp, \
             patch('to_data_library.data.bq.Client') as mock_bq:
            mock_ftp.return_value.download_file.return_value = '/tmp/file.csv'
            mock_bq.return_value.upload_table.return_value = None
            # Should not raise
            client.ftp_to_bq(
                ftp_connection_string='user:pass@host:21',
                ftp_filepath='/file.csv',
                bq_table='p.d.t',
                write_preference='truncate',
                bq_table_schema=valid_schema
            )
            # Invalid schema
            with self.assertRaises(ValueError):
                client.ftp_to_bq(
                    ftp_connection_string='user:pass@host:21',
                    ftp_filepath='/file.csv',
                    bq_table='p.d.t',
                    write_preference='truncate',
                    bq_table_schema=[('field1', 'NOTATYPE')]
                )

    def test_gs_parquet_to_bq_schema_validation(self):
        client = transfer.Client('fake_project_name')
        valid_schema = [('field1', 'STRING')]
        with patch('google.cloud.bigquery.Client'), \
             patch('google.cloud.bigquery.DatasetReference'), \
             patch('google.cloud.bigquery.TableReference'), \
             patch('google.cloud.bigquery.LoadJobConfig'):
            # Should not raise
            client.gs_parquet_to_bq(
                gs_uris="gs://bucket/file.parquet",
                table="p.d.t",
                write_preference="truncate",
                schema=valid_schema
            )
            # Invalid schema: wrong type
            with self.assertRaises(ValueError):
                client.gs_parquet_to_bq(
                    gs_uris="gs://bucket/file.parquet",
                    table="p.d.t",
                    write_preference="truncate",
                    schema=[('field1', 'NOTATYPE')]
                )

    def test_s3_to_bq_schema_validation(self):
        client = transfer.Client('fake_project_name')
        valid_schema = [('field1', 'STRING')]
        with patch('to_data_library.data.s3.Client') as mock_s3, \
             patch('to_data_library.data.bq.Client') as mock_bq, \
             patch('to_data_library.data.gs.Client') as mock_gs:
            mock_s3.return_value.download.return_value = '/tmp/file.csv'
            mock_bq.return_value.create_dataset.return_value = None
            mock_bq.return_value.load_table_from_uris.return_value = None
            mock_gs.return_value.upload.return_value = None
            # Should not raise
            client.s3_to_bq(
                aws_session=Mock(),
                bucket_name='bucket',
                object_name='file.csv',
                bq_table='p.d.t',
                write_preference='truncate',
                schema=valid_schema,
                gs_bucket_name='test-gcs-bucket',
                gs_file_name='test-file.csv'
            )
            # Invalid schema
            with self.assertRaises(ValueError):
                client.s3_to_bq(
                    aws_session=Mock(),
                    bucket_name='bucket',
                    object_name='file.csv',
                    bq_table='p.d.t',
                    write_preference='truncate',
                    schema=[('field1', 'NOTATYPE')],
                    gs_bucket_name='test-gcs-bucket',
                    gs_file_name='test-file.csv'
                )

    def test_s3_to_bq_partitioning_and_config(self):
        client = transfer.Client('fake_project_name')
        valid_schema = [('field1', 'STRING')]
        with patch('to_data_library.data.s3.Client') as mock_s3, \
             patch('to_data_library.data.bq.Client') as mock_bq, \
             patch('to_data_library.data.gs.Client') as mock_gs, \
             patch('os.remove') as mock_remove, \
             patch('os.path.exists', return_value=True):
            mock_s3.return_value.download.return_value = '/tmp/file.csv'
            mock_bq.return_value.create_dataset.return_value = None
            mock_bq.return_value.load_table_from_uris.return_value = None
            mock_gs.return_value.upload.return_value = None

            # Test with partition_date and partition_field, custom delimiter, source_format, max_bad_records
            client.s3_to_bq(
                aws_session=Mock(),
                bucket_name='bucket',
                object_name='file.csv',
                bq_table='p.d.t',
                write_preference='truncate',
                schema=valid_schema,
                partition_date='20240101',
                partition_field='field1',
                source_format='CSV',
                separator='|',
                max_bad_records=5,
                gs_bucket_name='test-gcs-bucket',
                gs_file_name='test-file.csv'
            )

            # Check that os.remove was called for cleanup
            mock_remove.assert_called_once_with('/tmp/file.csv')
            # Check that load_table_from_uris was called (table_id should have $20240101)
            args, kwargs = mock_bq.return_value.load_table_from_uris.call_args
            self.assertIn('$20240101', str(args[1].table_id))
            # Check job_config attributes
            job_config = kwargs['job_config']
            self.assertEqual(job_config.source_format, 'CSV')
            self.assertEqual(job_config.field_delimiter, '|')
            self.assertEqual(job_config.max_bad_records, 5)


class TestTransferEdgeCases(unittest.TestCase):
    def setUp(self):
        self.client = transfer.Client('fake_project')

    def test_gs_to_bq_invalid_source_format(self):
        with patch('google.cloud.bigquery.Client'), \
             patch('google.cloud.bigquery.DatasetReference'), \
             patch('google.cloud.bigquery.TableReference'), \
             patch('google.cloud.bigquery.LoadJobConfig'), \
             patch.object(transfer.logs.client.logger, 'error') as mock_error:
            with self.assertRaises(SystemExit):
                self.client.gs_to_bq(
                    gs_uris='gs://bucket/file.unknown',
                    table='p.d.t',
                    write_preference='truncate',
                    source_format='NOT_A_FORMAT'
                )
            mock_error.assert_called_with('Invalid SourceFormat entered: NOT_A_FORMAT')

    def test_gs_to_bq_partition_field_without_date(self):
        with patch('google.cloud.bigquery.Client'), \
             patch('google.cloud.bigquery.DatasetReference'), \
             patch('google.cloud.bigquery.TableReference'), \
             patch('google.cloud.bigquery.LoadJobConfig'), \
             patch.object(transfer.logs.client.logger, 'error') as mock_error:
            with self.assertRaises(SystemExit):
                self.client.gs_to_bq(
                    gs_uris='gs://bucket/file.csv',
                    table='p.d.t',
                    write_preference='truncate',
                    partition_field='field1'
                )
            mock_error.assert_called()

    def test_s3_to_bq_invalid_source_format(self):
        valid_schema = [('field1', 'STRING')]
        with patch('to_data_library.data.s3.Client'), \
             patch('to_data_library.data.bq.Client'), \
             patch('to_data_library.data.gs.Client') as mock_gs_client, \
             patch('os.path.exists', return_value=True), \
             patch('os.remove'), \
             patch.object(transfer.logs.client.logger, 'error') as mock_error:
            mock_gs_client.return_value.upload.return_value = None
            with self.assertRaises(ValueError):
                self.client.s3_to_bq(
                    aws_session=Mock(),
                    bucket_name='bucket',
                    object_name='file.csv',
                    bq_table='p.d.t',
                    write_preference='truncate',
                    schema=valid_schema,
                    partition_date='20240101',
                    partition_field='field1',
                    source_format='NOT_A_FORMAT',
                    separator='|',
                    max_bad_records=5,
                    gs_bucket_name='test-gcs-bucket',  # <-- new required argument
                    gs_file_name='test-file.csv'       # <-- optional
                )
            mock_error.assert_called_with('Invalid SourceFormat entered: NOT_A_FORMAT')

    def test_s3_to_bq_partition_field_without_date(self):
        valid_schema = [('field1', 'STRING')]
        with patch('to_data_library.data.s3.Client'), \
             patch('to_data_library.data.bq.Client'), \
             patch('to_data_library.data.gs.Client') as mock_gs_client, \
             patch('os.path.exists', return_value=True), \
             patch('os.remove'), \
             patch.object(transfer.logs.client.logger, 'error') as mock_error:
            mock_gs_client.return_value.upload.return_value = None
            with self.assertRaises(ValueError):
                self.client.s3_to_bq(
                    aws_session=Mock(),
                    bucket_name='bucket',
                    object_name='file.csv',
                    bq_table='p.d.t',
                    write_preference='truncate',
                    schema=valid_schema,
                    partition_field='field1',
                    source_format='CSV',
                    separator='|',
                    max_bad_records=5,
                    gs_bucket_name='test-gcs-bucket',  # <-- new required argument
                    gs_file_name='test-file.csv'       # <-- optional
                )
            mock_error.assert_called()

    def test_s3_to_gs_cleanup_and_logging(self):
        with patch('to_data_library.data.s3.Client') as mock_s3, \
             patch('to_data_library.data.gs.Client') as mock_gs, \
             patch('os.remove') as mock_remove, \
             patch('os.path.exists', return_value=True), \
             patch.object(transfer.logs.client.logger, 'info') as mock_info:
            mock_s3.return_value.download.return_value = '/tmp/file.csv'
            mock_gs.return_value.upload.return_value = None
            # Simulate one file found
            with patch.object(self.client, '_get_keys_in_s3_bucket', return_value=['file.csv']):
                self.client.s3_to_gs(
                    aws_session=Mock(),
                    s3_bucket_name='bucket',
                    s3_object_name='file.csv',
                    gs_bucket_name='gs-bucket',
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
