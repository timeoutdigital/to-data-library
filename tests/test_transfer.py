import unittest
import unittest.mock
from unittest.mock import ANY, Mock, patch

import parse

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
                      mock_bigqueryclient, mock_tablereference, mock_datasetrefererence):
        mock_default.return_value = 'first', 'second'
        client = transfer.Client('fake_project_name')
        client.gs_to_bq(
            gs_uris="gs://{}/{}".format('fake_bucket_name', 'sample.csv'),
            table='{}.{}.{}'.format('fake_project_name', 'fake_dataset_id', 'fake_table_id'),
            write_preference='truncate',
            max_bad_records=10
        )

        mock_datasetrefererence.assert_called_with(project='fake_project_name', dataset_id='fake_dataset_id')
        mock_tablereference.assert_called_with(ANY, table_id='fake_table_id')
        mock_loadjobconfig.assert_called_with(source_format='CSV',
                                              skip_leading_rows=1,
                                              autodetect=True,
                                              field_delimiter=',',
                                              write_disposition='WRITE_TRUNCATE',
                                              allow_quoted_newlines=True,
                                              max_bad_records=10)

    @patch('boto3.client')
    @patch('boto3.resource')
    @patch('to_data_library.data.bq.default')
    @patch('google.cloud.storage.Client')
    def test_s3_to_gs(self, mock_storage, mock_default, mock_s3_resource, mock_s3_boto):
        mock_default.return_value = 'first', 'second'
        client = transfer.Client(project=setup.project)
        client.s3_to_gs(s3_connection_string="{}:{}:{}".format('fake_s3_region', 'fake_s3_access_key',
                                                               'fake_s3_secret_key'),
                        s3_bucket_name='fake_s3_bucket',
                        s3_object_name='download_sample.csv',
                        gs_bucket_name='fake_gs_bucket_name',
                        gs_file_name='transfer_s3_to_gs.csv')

        mock_s3_boto.assert_called_with('s3',
                                        region_name='fake_s3_region',
                                        aws_access_key_id='fake_s3_access_key',
                                        aws_secret_access_key='fake_s3_secret_key')

        mock_s3_resource.assert_called_with(service_name='s3', region_name='fake_s3_region')

    @patch('boto3.client')
    def test_get_keys_in_s3_bucket(self, mock_boto):
        parsed_connection = parse.parse(
            '{region}:{access_key}:{secret_key}', "{}:{}:{}".format('fake_s3_region', 'fake_s3_access_key',
                                                                    'fake_s3_secret_key'))
        client = transfer.Client(project=setup.project)
        client._get_keys_in_s3_bucket(parsed_connection, 'fake_bucket_name', 'fake_prefix_name')

        mock_boto.assert_called_with('s3',
                                     region_name='fake_s3_region',
                                     aws_access_key_id='fake_s3_access_key',
                                     aws_secret_access_key='fake_s3_secret_key')
