import csv
import unittest

from google.cloud import bigquery, storage

from tests.setup import setup
from to_data_library.data import transfer


def setUpModule():
    setup.create_bq_table()
    setup.create_bucket()
    setup.upload_s3_files()


def tearDownModule():
    setup.delete_bq_dataset()
    setup.delete_bucket()
    setup.cleanup()
    setup.remove_s3_files()


class TestTransfer(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestTransfer, self).__init__(*args, **kwargs)
        self.setup = setup

    def test_bq_to_gs(self):

        client = transfer.Client(project=self.setup.project)
        file_names = client.bq_to_gs(
            table='{}.{}.{}'.format(self.setup.project, self.setup.dataset_id, 'actors'),
            bucket_name=self.setup.bucket_name,
        )

        # creating list based on source big query table values
        bq_keys = []
        bigquery_client = bigquery.Client(project=self.setup.project)
        job = bigquery_client.query(
            'SELECT profile_id, first_name, last_name from {}.{}'.format(
                self.setup.dataset_id,
                'actors'
            )
        )
        for row in job.result():
            bq_keys.append('{}{}{}'.format(
                row.profile_id,
                '' if row.first_name is None else row.first_name,
                '' if row.last_name is None else row.last_name,
            ))

        # creating list based on destination bucket file values
        storage_client = storage.Client(project=self.setup.project)
        with open('actors.csv', 'wb') as file_obj:
            storage_client.download_blob_to_file(blob_or_uri=file_names[0], file_obj=file_obj)

        with open('actors.csv', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            storage_keys = []
            for row in reader:
                storage_keys.append(
                    str(row['profile_id']) + row['first_name'] + row['last_name']
                )

        self.assertEqual(storage_keys, bq_keys)

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

    def test_s3_to_gs(self):

        client = transfer.Client(project=setup.project)
        client.s3_to_gs(s3_connection_string="{}:{}:{}".format(setup.s3_region, setup.s3_access_key,
                                                               setup.s3_secret_key),
                        s3_bucket_name=setup.s3_bucket,
                        s3_object_name='download_sample.csv',
                        gs_bucket_name=setup.bucket_name,
                        gs_file_name='transfer_s3_to_gs.csv')

        gs_client = storage.Client()
        bucket = gs_client.bucket(setup.bucket_name)
        self.assertTrue(storage.Blob(name='transfer_s3_to_gs.csv', bucket=bucket).exists(gs_client))
