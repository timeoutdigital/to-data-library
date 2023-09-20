import csv
import unittest
import unittest.mock
from unittest.mock import Mock, patch

from google.cloud import bigquery, storage

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

    def test_gs_to_bq(self):

        client = transfer.Client(project=self.setup.project)
        client.gs_to_bq(
            gs_uris="gs://{}/{}".format(self.setup.bucket_name, 'sample.csv'),
            table='{}.{}.{}'.format(self.setup.project, self.setup.dataset_id, 'actors_from_gs'),
            write_preference='truncate'
        )

        # creating list based on source bucket file values
        storage_client = storage.Client(project=self.setup.project)
        with open('actors_from_gs.csv', 'wb') as file_obj:
            storage_client.download_blob_to_file(
                blob_or_uri="gs://{}/{}".format(self.setup.bucket_name, 'sample.csv'),
                file_obj=file_obj
            )
        with open('actors_from_gs.csv', newline='') as csvfile:
            reader = csv.reader(csvfile)
            storage_keys = []
            for row in reader:
                storage_keys.append(
                    "{}{}{}".format(
                        row[0],
                        "NULL" if row[1] else row[1],
                        "NULL" if row[2] else row[2],
                    )
                )

        # creating list based on destination big query table values
        bq_keys = []
        bigquery_client = bigquery.Client(project=self.setup.project)
        job = bigquery_client.query(
            'SELECT profile_id, first_name, last_name from {}.{}'.format(
                self.setup.dataset_id,
                'actors_from_gs'
            )
        )
        for row in job.result():
            bq_keys.append('{}{}{}'.format(
                row.profile_id,
                'NULL' if row.first_name is None else row.first_name,
                'NULL' if row.last_name is None else row.last_name,
            ))

    @patch('boto3.client')
    @patch('boto3.resource')
    def test_s3_to_gs(self, mock_resource, mock_parse, mock_boto):
        client = transfer.Client(project=setup.project)
        client.s3_to_gs(s3_connection_string="{}:{}:{}".format('fake_s3_region', 'fake_s3_access_key',
                                                               'fake_s3_secret_key'),
                        s3_bucket_name='fake_s3_bucket',
                        s3_object_name='download_sample.csv',
                        gs_bucket_name='fake_gs_bucket_name',
                        gs_file_name='transfer_s3_to_gs.csv')

        # gs_client = storage.Client()
        # bucket = gs_client.bucket(setup.bucket_name)
        # self.assertTrue(storage.Blob(name='transfer_s3_to_gs.csv', bucket=bucket).exists(gs_client))

    def test_get_keys_in_s3_bucket(self):
        pass
