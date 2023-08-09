import unittest
import boto3
import csv
import pysftp
from google.cloud import bigquery
from google.cloud import storage

from to_data_library.data import transfer
from tests.setup import setup


def setUpModule():
    setup.create_bq_table()
    setup.create_bucket()
   #setup.start_sftp_instance()
    #setup.upload_ftp_files()
    #setup.upload_s3_files()


def tearDownModule():
    setup.delete_bq_dataset()
    setup.delete_bucket()
    setup.cleanup()
    #setup.remove_ftp_files()
    #setup.stop_sftp_instance()
    #setup.remove_s3_files()


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

    '''def test_ftp_to_bq(self):

        client = transfer.Client(self.setup.project)
        client.ftp_to_bq(
            ftp_connection_string=f"{self.setup.ftp_username}:{self.setup.ftp_password}@{self.setup.ftp_hostname}:{self.setup.ftp_port}",
            bq_table=f"{self.setup.project}.{self.setup.dataset_id}.ftp_test_load",
            ftp_filepath="test_dir/download_sample.csv",
            write_preference='truncate',
            bq_table_schema=(('profileid', 'STRING'), ('firstname', 'STRING'), ('lastname', 'STRING'))
        )

        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        connection = pysftp.Connection(
            host=self.setup.ftp_hostname,
            username=self.setup.ftp_username,
            password=self.setup.ftp_password,
            port=self.setup.ftp_port,
            cnopts=cnopts
        )
        connection.get("test_dir/download_sample.csv", 'tests/data/ftp_to_bq_test_sample.csv')
        ftp_keys = []
        with open("tests/data/ftp_to_bq_test_sample.csv", 'rt') as f:
            reader = csv.reader(f)
            reader.__next__()
            for row in reader:
                ftp_keys.append(
                    "{}{}{}".format(
                        row[0],
                        row[1] if row[1] else "NULL",
                        row[2] if row[2] else "NULL",
                    )
                )

        bq_keys = []
        bigquery_client = bigquery.Client(project=self.setup.project)
        job = bigquery_client.query(
            'SELECT profileid, firstname, lastname from {}.{}'.format(
                self.setup.dataset_id,
                'ftp_test_load'
            )
        )
        for row in job.result():
            bq_keys.append('{}{}{}'.format(
                row.profileid,
                'NULL' if row.firstname is None else row.firstname,
                'NULL' if row.lastname is None else row.lastname,
            ))

        self.assertListEqual(bq_keys, ftp_keys)

    def test_bq_to_ftp(self):

        cli = transfer.Client(self.setup.project)
        cli.bq_to_ftp(
            ftp_connection_string=f"{self.setup.ftp_username}:{self.setup.ftp_password}@{self.setup.ftp_hostname}:{self.setup.ftp_port}",
            bq_table=f"{self.setup.project}.{self.setup.dataset_id}.{self.setup.table_id}",
            ftp_filepath="test_dir/bq_to_ftp_test_sample.csv",
            separator="|",
            print_header=False
        )

        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        connection = pysftp.Connection(
            host=self.setup.ftp_hostname,
            username=self.setup.ftp_username,
            password=self.setup.ftp_password,
            port=self.setup.ftp_port,
            cnopts=cnopts
        )
        connection.get("test_dir/bq_to_ftp_test_sample.csv", 'tests/data/bq_to_ftp_test_sample.csv')
        ftp_keys = []
        with open("tests/data/bq_to_ftp_test_sample.csv", 'rt') as f:
            reader = csv.reader(f, delimiter="|")
            for row in reader:
                ftp_keys.append(
                    "{}{}{}".format(
                        row[0],
                        "NULL" if row[1] is None else row[1],
                        "NULL" if row[2] is None else row[2],
                    )
                )

        bq_keys = []
        bigquery_client = bigquery.Client(project=self.setup.project)
        job = bigquery_client.query(
            'SELECT profile_id, first_name, last_name from {}.{}'.format(
                self.setup.dataset_id,
                self.setup.table_id
            )
        )
        for row in job.result():
            bq_keys.append('{}{}{}'.format(
                row.profile_id,
                'NULL' if row.first_name is None else row.first_name,
                'NULL' if row.last_name is None else row.last_name,
            ))

    def test_gs_to_s3(self):

        client = transfer.Client(project=setup.project)
        client.gs_to_s3(gs_uri=f'gs://{setup.bucket_name}/{setup.bucket_blob_name}',
                        s3_connection_string="{}:{}:{}".format(setup.s3_region, setup.s3_access_key,
                                                               setup.s3_secret_key),
                        s3_bucket=setup.s3_bucket)

        s3_client = boto3.resource(service_name='s3',
                                   region_name=self.setup.s3_region,
                                   aws_access_key_id=self.setup.s3_access_key,
                                   aws_secret_access_key=self.setup.s3_secret_key)
        bucket = s3_client.Bucket(setup.s3_bucket)
        obj = list(bucket.objects.filter(Prefix=setup.bucket_blob_name))
        self.assertTrue(any([w.key == setup.bucket_blob_name for w in obj]))

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
'''