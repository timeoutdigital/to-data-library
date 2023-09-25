import csv
import uuid

# from google.auth import default
import google.auth
from google.cloud import bigquery, storage
from jinja2 import Template

from to_data_library.data import logs, transfer
from to_data_library.data._helper import get_bq_write_disposition


class Client:
    """
    Client to bundle BigQuery functionality.

    Args:
        project (str): The Project ID for the project which the client acts on behalf of.
    """

    def __init__(self, project):
        self.project = project

        scopes = (
            'https://www.googleapis.com/auth/bigquery',
            'https://www.googleapis.com/auth/cloud-platform',
            'https://www.googleapis.com/auth/drive',
        )
        credentials, _ = google.auth.default(scopes=scopes)
        self.bigquery_client = bigquery.Client(
            credentials=credentials,
            project=self.project
        )

    def download_table(self, table, local_folder='.', separator=',', print_header=True):
        """
        Export the table to the local file in CSV format.

        Args:
            table (str): The BigQuery table name. For example: ``project.dataset.table``.
            local_folder (:obj:`str`, Optional):  The local folder with out the slash at end. For example:
              ``/some_path/some_inside_path``. Defaults to current path :data:`.`
            separator (:obj:`str`, Optional): The separator. Defaults to :data:`,`
            print_header (:obj:`boolean`, optional):  True to print a header row in the exported file otherwise False.
              Defaults to :data:`True`.

        Returns:
            list: list of file names, if the table is big, multiple files are downloaded

        Examples:
            >>> from to_data_library.data import bq
            >>> client = bq.Client(project='my-project-id')
            >>> client.download_table(table='my-project-id.my_dataset.my_table')

        """
        storage_client = storage.Client(project=self.project)
        transfer_client = transfer.Client(project=self.project)

        # Create tmp bucket and transfer table from BQ to a temporary bucket in GCS
        tmp_bucket = self._create_tmp_bucket_in_gcs(storage_client)
        transfer_client.bq_to_gs(table, tmp_bucket.name, separator=separator, print_header=print_header)

        # Get iterator object for all blobs in the tmp bucket that were transferred
        logs.client.logger.info('Getting the list of available blobs in gs://{}'.format(tmp_bucket.name))
        blobs = storage_client.list_blobs(tmp_bucket.name)

        blob_names = []
        for blob in blobs:
            logs.client.logger.info('Downloading gs://{}/{}'.format(tmp_bucket.name, blob.name))
            blob.download_to_filename('{}/{}'.format(local_folder, blob.name))
            blob_names.append(blob.name)
            logs.client.logger.info('Deleting gs://{}/{}'.format(tmp_bucket.name, blob.name))
            blob.delete()

        logs.client.logger.info('Deleting bucket gs://{}'.format(tmp_bucket.name))
        tmp_bucket.delete()

        return blob_names

    def _create_tmp_bucket_in_gcs(self, storage_client):
        """ Creates a temporary bucket in GCS using a storage client as an input
        Args:
            storage_client (Client):  A client on google storage"""
        bucket_name = str(uuid.uuid4())
        logs.client.logger.info('Creating temporary bucket gs://{}'.format(bucket_name))
        tmp_bucket = storage_client.create_bucket(bucket_name, location='EU')
        logs.client.logger.info('Done')
        return tmp_bucket

    def upload_table(self, file_path, table, write_preference, separator=',', auto_detect=True, skip_leading_rows=True,
                     schema=(), partition_date=None, partition_field=None, max_bad_records=0):
        """Import into the BigQuery table from the local file.

        Args:
            file_path (str):  The local file path
            table (str): The BigQuery table name. For example: ``project.dataset.table``.
            write_preference (str): The option to specify what action to take when you load data from a source file.
              Value can be on of
                                              ``'empty'``: Writes the data only if the table is empty.
                                              ``'append'``: Appends the data to the end of the table.
                                              ``'truncate'``: Erases all existing data in a table before writing the
                                                new data.
            separator (str, Optional): The separator. Defaults to :data:`,`
            auto_detect (boolean, Optional):  True if the schema should automatically be detected otherwise False.
              Defaults to :data:`True`.
            skip_leading_rows (boolean, Optional):  True to skip the first row of the file otherwise False. Defaults to
              :data:`True`.
            schema (tuple, Optional): The BigQuery table schema. For example: ``(('first_field','STRING'),
            ('second_field', 'STRING'))``. Defaults to ().
            partition_date (str, Optional): The ingestion date for partitioned destination table. For example:
              ``20210101``. The partition field name will be __PARTITIONTIME
            partition_field (str, Optional): The field on which the destination table is partitioned. The field must be
              a top-level TIMESTAMP or DATE field. Only used if partition_date is not set.
            max_bad_records (int, Optional): The maximum number of invalid rows. Defaults to :data:`0`.

        Examples:
            >>> from to_data_library.data import bq
            >>> client = bq.Client(project='my-project-id')
            >>> client.upload_table(file_path='/my_path/my_filename', table='my-project-id.my-dataset.my-table')

        """

        project, dataset_id, table_id = table.split('.')
        dataset_ref = bigquery.DatasetReference(project=project, dataset_id=dataset_id)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1 if skip_leading_rows else 0,
            autodetect=auto_detect if not schema else False,
            field_delimiter=separator,
            write_disposition=get_bq_write_disposition(write_preference),
            allow_quoted_newlines=True,
            max_bad_records=max_bad_records
        )

        if schema:
            job_config.schema = [bigquery.SchemaField(schema_field[0], schema_field[1]) for schema_field in schema]

        if partition_date:
            job_config.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY)
            table_id += '${}'.format(partition_date)
        elif partition_field:
            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field=partition_field)

        table_ref = bigquery.TableReference(dataset_ref, table_id=table_id)

        with open(file_path, "rb") as source_file:
            logs.client.logger.info('Loading BigQuery table {} from file {}'.format(table, file_path))
            job = self.bigquery_client.load_table_from_file(source_file, table_ref, job_config=job_config)

        job.result()

    def run_query(self, query=None, query_file_name=None, params=(), destination=None, write_preference='empty',
                  partition_date=None):
        """Run the query and return the result

        Args:
            query (str) or query_file_name(str) is required.
            query (str, Optional): The query string.
            query_file_name (str, Optional): The query file path. For example: ``/my_query_path/my_query.sql``
                The use of variables in an SQL string can be done using ``{{var}}``.
                For example: ``SELECT first_name FROM profile where last_name={{last_name}}``
                More options of interactions, please read Jinja Documentation at
                https://jinja.palletsprojects.com/en/2.11.x/templates/
            params (dict, Optional): The query parameters. Each element should be {param_name: param_value}.
                For example: ``{"last_name", "Deniro", "age": 80}``
            destination (str, Optional): The BigQuery destination table name. For example: ``project.dataset.table``.
              Defaults to None
            write_preference (str, Optional): The option to specify what action to take when you load data from a
              source file. Value can be on of
                      ``'empty'``: Writes the data only if the table is empty.
                      ``'append'``: Appends the data to the end of the table.
                      ``'truncate'``: Erases all existing data in a table before writing the new data.
                      Defaults to :data:`empty`
            partition_date (str, Optional): The ingestion date for partitioned destination table. For example:
              ``20210101``. The partition field name will be __PARTITIONTIME

        Returns:
            The query results

        """
        if not (query or query_file_name):
            raise Exception("Parameter [query] or [query_file_name] must be provided")

        job_config = bigquery.QueryJobConfig(allow_large_results=True)
        if destination:
            if partition_date:
                job_config.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY)
                destination += '${}'.format(partition_date)
            job_config.destination = destination
            job_config.write_disposition = get_bq_write_disposition(write_preference)

        if query_file_name:
            with open(query_file_name, mode="r") as query_file:
                query = query_file.read()
        if params:
            query_template = Template(query)
            query = query_template.render(params)

        if destination:
            logs.client.logger.info(
                ('Writing query results into {destination}: \n' + 75 * '=' + '\n{query}\n' + 75 * '=').format(
                    destination=destination,
                    query=query
                )
            )
        else:
            logs.client.logger.info(('Running query: \n' + 75 * '=' + '\n{}\n' + 75 * '=').format(query))

        query_job = self.bigquery_client.query(query, job_config=job_config)
        result = query_job.result()
        logs.client.logger.info(
            'Total bytes processed: {:.2f} GiB'.format(query_job.total_bytes_processed / (1024 * 1024 * 1024))
        )

        return result

    def create_table(self, table, schema_file_name=None, schema_fields=None):
        """Create the BigQuery table

        Args:
            table (str): The BigQuery table name. For example: ``project.dataset.table``.
            schema_file_name (str, Optional): The schema file name. For example: ``/my_schema_path/schema.csv``.
                                              The schema file should be in CSV format and each line is FIELD_NAME,
                                                FIELD_TYPE, MODE. For example:
                                              ``id,STRING,REQUIRED``. Defaults to None
            schema_fields (tuple, Optional): The table schema.
                                             For example. ``(("id", "INT64", "REQUIRED"),("name", "STRING", "NULLABLE"))
                                             ``. Defaults to None.

        """

        if not schema_fields and not schema_file_name:
            raise ValueError('Either schema file name or schema fields should be provided.')

        if schema_file_name:
            with open(schema_file_name) as schema_file:
                reader = csv.reader(schema_file)
                schema = [
                    bigquery.SchemaField(row[0], row[1], mode=row[2])
                    for row in reader
                ]
        else:
            schema = [
                bigquery.SchemaField(schema_field[0], schema_field[1], mode=schema_field[2])
                for schema_field in schema_fields
            ]

        table = bigquery.Table(table, schema=schema)
        self.bigquery_client.create_table(table)

    def delete_table(self, table):
        """Delete the BigQuery table

        Args:
            table (str): The BigQuery table name. For example: ``project.dataset.table``.
        """
        self.bigquery_client.delete_table(table)

    def create_dataset(self, dataset, location='EU'):
        """Create the BigQuery dataset

        Args:
            dataset (str): The BigQuery dataset name. For example: ``my-project.my_dataset``.
            location (str, Optional): The location of the dataset. Defaults to EU.
        """
        dataset = bigquery.Dataset(f'{self.project}.{dataset}')
        dataset.location = location
        self.bigquery_client.create_dataset(dataset)

    def delete_dataset(self, dataset, delete_contents=False):
        """Delete the BigQuery dataset

        Args:
            dataset (str): The BigQuery dataset name. For example: ``my-project.my_dataset``.
            delete_contents (boolean, Optional): If True, delete all the tables in the dataset. If
                False and the dataset contains tables, the request will fail.
                Default is False.
        """
        self.bigquery_client.delete_dataset(dataset, delete_contents=delete_contents)
