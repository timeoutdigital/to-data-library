import glob
import os

import boto3
from google.cloud import bigquery, exceptions, storage


class Setup:

    def __init__(self):

        self.project = os.environ.get('PROJECT', None)
        self.dataset_id = os.environ.get('DATASET_ID', None)
        self.table_id = os.environ.get('TABLE_ID', None)
        self.bucket_name = os.environ.get('BUCKET_NAME', None)
        self.s3_region = 'eu-west-1'
        self.s3_bucket = 'dummy-bucket'

    def create_bq_table(self):
        dataset_ref = bigquery.DatasetReference(project=self.project, dataset_id=self.dataset_id)
        table_ref = bigquery.TableReference(dataset_ref, table_id=self.table_id)
        bigquery_client = bigquery.Client(project=self.project)

        try:
            bigquery_client.get_dataset(dataset_ref)
            self.delete_bq_dataset()
        except exceptions.NotFound:
            pass
        finally:
            bigquery_client.create_dataset(dataset_ref)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            field_delimiter=','
        )

        with open('tests/data/sample.csv', "rb") as source_file:
            job = bigquery_client.load_table_from_file(source_file, table_ref, job_config=job_config)
        job.result()

    def delete_bq_dataset(self):
        bigquery_client = bigquery.Client(project=self.project)
        bigquery_client.delete_dataset(self.dataset_id, delete_contents=True)

    def upload_s3_files(self):
        s3_client = boto3.resource(
            service_name='s3',
            region_name=self.s3_region
        )
        bucket = s3_client.Bucket(self.s3_bucket)
        bucket.upload_file('tests/data/sample.csv', 'download_sample.csv')

    def remove_s3_files(self):
        s3_client = boto3.resource(service_name='s3',
                                   region_name=self.s3_region,
                                   aws_access_key_id=self.s3_access_key,
                                   aws_secret_access_key=self.s3_secret_key)

        s3_client.Bucket(self.s3_bucket).Object('download_sample.csv').delete()
        s3_client.Bucket(self.s3_bucket).Object('sample.csv').delete()
        s3_client.Bucket(self.s3_bucket).Object('s3_upload_file.csv').delete()

    def create_bucket(self):
        storage_client = storage.Client(project=self.project)

        try:
            storage_client.get_bucket(self.bucket_name)
            self.delete_bucket()
        except exceptions.NotFound:
            pass
        finally:
            bucket = storage_client.create_bucket(self.bucket_name, location='EU')

        blob = bucket.blob('sample.csv')
        blob.upload_from_filename('tests/data/sample.csv')

    def delete_bucket(self):

        storage_client = storage.Client(project=self.project)
        bucket = storage_client.get_bucket(self.bucket_name)

        blobs = storage_client.list_blobs(self.bucket_name)
        for blob in blobs:
            blob.delete()

        bucket.delete()

    def cleanup(self):
        for x in glob.glob("*.csv"):
            os.remove(x)
        for x in glob.glob("actors*"):
            os.remove(x)


setup = Setup()
