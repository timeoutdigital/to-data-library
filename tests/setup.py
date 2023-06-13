import os
from google.cloud import bigquery
from google.cloud import exceptions


class Setup:

    def __init__(self):

        self.project = os.environ.get('PROJECT', None)
        self.dataset_id = os.environ.get('DATASET_ID', None)
        self.table_id = os.environ.get('TABLE_ID', None)

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


setup = Setup()
