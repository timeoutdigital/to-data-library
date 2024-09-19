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
@patch('google.cloud.bigquery.Client')
@patch('google.cloud.bigquery.LoadJobConfig')
@patch('to_data_library.data.bq.default')
def test_gs_to_bq(self, mock_default, mock_loadjobconfig,
                  mock_bigqueryclient, mock_tablereference, mock_datasetreference):

    mock_default.return_value = 'first', 'second'

    # Mock return values for LoadJobConfig
    mock_loadjobconfig.return_value.source_format = 'CSV'
    mock_loadjobconfig.return_value.skip_leading_rows = 1
    mock_loadjobconfig.return_value.autodetect = True
    mock_loadjobconfig.return_value.field_delimiter = ','
    mock_loadjobconfig.return_value.write_disposition = 'WRITE_TRUNCATE'
    mock_loadjobconfig.return_value.allow_quoted_newlines = True
    mock_loadjobconfig.return_value.max_bad_records = 10

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
    mock_bigqueryclient.return_value.load_table_from_uris.assert_called_once_with(
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
