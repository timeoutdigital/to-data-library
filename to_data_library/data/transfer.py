import os

from google.api_core import exceptions
from google.cloud import bigquery, storage

from to_data_library.data import bq, ftp, gs, logs, s3
from to_data_library.data._helper import get_bq_write_disposition, merge_files


class Client:
    """
    Client to bundle transfers from a source to destination.

    Args:
        project (str): The Project ID for the project which the client acts on behalf of.
    """

    def __init__(self, project):
        self.project = project

    def bq_to_gs(self, table, bucket_name, separator=',', print_header=True, compress=False):
        """Extract BigQuery table into the GoogleStorage

        Args:
            table (str):  The BigQuery table name. For example: ``my-project-id.you-dataset.my-table``
            bucket_name (str):  The name of the bucket in GoogleStorage.
            separator (:obj:`str`, optional): The separator. Defaults to :data:`,`.
            print_header (:obj:`boolean`, optional):  True to print a header row in the exported data otherwise False.
              Defaults to :data:`True`.
            compress (:obj:`boolean`, optional): True to apply a GZIP compression. False to export without compression.

        Returns:
            list: The list of GoogleStorage paths for the uploaded files into the GoogleStorage.
            if the table is big, the exported files will be multiple.

        Examples:
            >>> from to_data_library.data import transfer
            >>> client = transfer.Client(project='my-project-id')
            >>> client.bq_to_gs('my-project-id.some_dataset.some_table', 'some-bucket-name')
        """

        project, dataset_id, table_id = table.split('.')
        dataset_ref = bigquery.DatasetReference(
            project=project, dataset_id=dataset_id)
        table_ref = bigquery.TableReference(dataset_ref, table_id=table_id)

        bq_client = bigquery.Client(project=self.project)
        logs.client.logger.info('Extracting from {table} to gs://{bucket_name}/{table_id}_*'.format(
            bucket_name=bucket_name, table_id=table_id, table=table)
        )
        extract_job = bq_client.extract_table(
            source=table_ref,
            destination_uris='gs://{bucket_name}/{table_id}_*'.format(
                bucket_name=bucket_name, table_id=table_id),
            job_config=bigquery.ExtractJobConfig(
                field_delimiter=separator,
                print_header=print_header,
                compression=bigquery.Compression.GZIP if compress else None
            )
        )
        extract_job.result()
        storage_client = storage.Client(project=self.project)
        logs.client.logger.info(
            'Getting the list of available blobs in gs://{}'.format(bucket_name))
        blobs = storage_client.list_blobs(bucket_name)

        return ['gs://{}/{}'.format(bucket_name, blob.name) for blob in blobs]

    def gs_to_bq(self, gs_uris, table, write_preference, auto_detect=True, skip_leading_rows=True, separator=',',
                 schema=(), partition_date=None, max_bad_records=0):
        """Load file from Google Storage into the BigQuery table

        Args:
            gs_uris (Union[str, Sequence[str]]):  The Google Storage uri(s) for the file(s). For example: A single file:
              ``gs://my_bucket_name/my_filename``, multiple files: ``[gs://my_bucket_name/my_first_file,
              gs://my_bucket_name/my_second_file]``.
            table (str): The BigQuery table name. For example: ``project.dataset.table``.
            write_preference (str): The option to specify what action to take when you load data from a source file.
            Value can be on of
                                              ``'empty'``: Writes the data only if the table is empty.
                                              ``'append'``: Appends the data to the end of the table.
                                              ``'truncate'``: Erases all existing data in a table before writing the
                                                new data.
            auto_detect (boolean, Optional):  True if the schema should automatically be detected otherwise False.
            Defaults to :data:`True`.
            skip_leading_rows (boolean, Optional):  True to skip the first row of the file otherwise False. Defaults to
              :data:`True`.
            separator (str, Optional): The separator. Defaults to :data:`,`
            schema (tuple): The BigQuery table schema. For example: ``(('first_field','STRING'),('second_field',
              'STRING'))``
            partition_date (str, Optional): The ingestion date for partitioned BigQuery table. For example: ``20210101``
            . The partition field name will be __PARTITIONTIME.
            max_bad_records (int, Optional): The maximum number of rows with errors. Defaults to :data:0

        Examples:
            >>> from to_data_library.data import transfer
            >>> client = transfer.Client(project='my-project-id')
            >>> client.gs_to_bq(gs_uris='gs://my-bucket-name/my-filename',table='my-project-id.my_dataset.my_table')
        """

        project, dataset_id, table_id = table.split('.')
        dataset_ref = bigquery.DatasetReference(
            project=project, dataset_id=dataset_id)
        table_ref = bigquery.TableReference(dataset_ref, table_id=table_id)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1 if skip_leading_rows else 0,
            autodetect=auto_detect,
            field_delimiter=separator,
            write_disposition=get_bq_write_disposition(write_preference),
            allow_quoted_newlines=True,
            max_bad_records=max_bad_records
        )

        if partition_date:
            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY)
            table_id += '${}'.format(partition_date)

        bq_client = bq.Client(project)
        try:
            bq_client.create_dataset(dataset_id)
        except exceptions.Conflict:
            logs.client.logger.info(
                'Dataset {} Already exists'.format(dataset_id))

        if schema:
            job_config.schema = [bigquery.SchemaField(
                schema_field[0], schema_field[1]) for schema_field in schema]

        bigquery_client = bigquery.Client(project=self.project)
        logs.client.logger.info(
            'Loading BigQuery table {} from {}'.format(table, gs_uris))
        job = bigquery_client.load_table_from_uri(
            gs_uris, table_ref, job_config=job_config)

        job.result()

    def ftp_to_bq(self, ftp_connection_string, ftp_filepath, bq_table, write_preference, separator=',',
                  skip_leading_rows=True, bq_table_schema=None, partition_date=None):
        """Export from FTP to BigQuery

        Args:
            ftp_connection_string (str): The FTP connection string in the format {username}:{password}@{host}:{port}
            bq_table (str): The BigQuery table. For example: ``my-project-id.my-dataset.my-table``
            write_preference (str): The option to specify what action to take when you load data from a source file.
              Value can be on of
                                              ``'empty'``: Writes the data only if the table is empty.
                                              ``'append'``: Appends the data to the end of the table.
                                              ``'truncate'``: Erases all existing data in a table before writing the
                                                new data.
            ftp_filepath (str): The path to the file to download.
            separator (:obj:`str`, Optional): The separator. Defaults to :data:`,`.
            skip_leading_rows (boolean, Optional):  True to skip the first row of the file otherwise False. Defaults to
              :data:`True`.
            bq_table_schema (tuple, Optional): The BigQuery table schema. For example: ``(('first_field','STRING'),
            ('second_field','STRING'))``
            partition_date (str, Optional): The ingestion date for partitioned BigQuery table. For example: ``20210101``
            . The partition field name will be __PARTITIONTIME

        Examples:
            >>> from to_data_library.data import transfer
            >>> client = transfer.Client(project='my-project-id')
            >>> client.ftp_to_bq(
            >>>     ftp_connection_string='username:password@hots:port',
            >>>     ftp_filepath='/my-path/to-the-ftp-file',
            >>>     bq_table='my-project-id.my-dataset.my-table'
            >>> )

        """

        # download the ftp file
        ftp_client = ftp.Client(connection_string=ftp_connection_string)
        local_file = ftp_client.download_file(ftp_filepath)

        # upload the ftp file into BigQuery
        bq_client = bq.Client(project=self.project)
        bq_client.upload_table(
            file_path=local_file,
            table=bq_table,
            separator=separator,
            skip_leading_rows=skip_leading_rows,
            write_preference=write_preference,
            schema=bq_table_schema,
            partition_date=partition_date
        )

    def bq_to_ftp(self, bq_table, ftp_connection_string, ftp_filepath, separator=',', print_header=True):
        """Export from BigQuery to FTP

        Args:
            bq_table (str): The BigQuery table. For example: ``my-project-id.my-dataset.my-table``
            ftp_connection_string (str): The FTP connection string in the format {username}:{password}@{host}:{port}
            ftp_filepath (str): The path to the file to download.
            separator (:obj:`str`, optional): The separator. Defaults to :data:`,`.
            print_header (boolean, Optional):  True to write header for the CSV file, otherwise False. Defaults to :
            data:`True`.

        Examples:
            >>> from to_data_library.data import transfer
            >>> client = transfer.Client(project='my-project-id')
            >>> client.bq_to_ftp(
            >>>     bq_table='my-project-id.my-dataset.my-table',
            >>>     ftp_connection_string='username:password@hots:port',
            >>>     ftp_filepath='/my-path/to-the-ftp-file'
            >>> )

        """
        # download the the BigQuery table into local
        bq_client = bq.Client(project=self.project)
        local_files = bq_client.download_table(
            table=bq_table,
            separator=separator,
            print_header=print_header
        )

        # merge the files if they are more than one
        if len(local_files) > 1:
            logs.client.logger.info('Merging {}'.format(','.join(local_files)))
            merged_file = merge_files(local_files)
        else:
            merged_file = local_files[0]

        # upload the merged file
        ftp_client = ftp.Client(connection_string=ftp_connection_string)
        ftp_client.upload_file(local_path=merged_file,
                               remote_path=ftp_filepath)

    def gs_to_s3(self, aws_session, gs_uri, s3_bucket):
        """
        Exports file from Google storage bucket to S3 bucket

        Args:
        aws_session: authenticated AWS session.
        gs_uri (str): Google storage uri path
        s3_connection_string (str): The S3 connection string in the format
                                    {region}:{access_key}:{secret_key}
        s3_bucket (str): s3 bucket name

        Example:
            >>> from to_data_library.data import transfer
            >>> client = transfer.Client(project='my-project-id')
            >>> client.gs_to_s3(aws_session,
            >>>                 gs_uri='gs://my-bucket-name/my-filename',
            >>>                 s3_bucket='bucket_name')
        """

        local_file = os.path.basename(gs_uri)
        gs_client = gs.Client(self.project)
        gs_client.download(gs_uri, local_file)

        s3_client = s3.Client(aws_session)
        s3_client.upload(local_file,
                         s3_bucket)

    def s3_to_gs(self, aws_session, s3_bucket_name,
                 s3_object_name, gs_bucket_name, gs_file_name=None):
        """
        Exports file(s) from S3 bucket to Google storage bucket

        Args:
          aws_session: authenticated AWS session.
          s3_bucket_name (str): s3 bucket name
          s3_object_name (str): s3 object name or prefix to match multiple files to copy
          gs_bucket_name (str): Google storage bucket name
          gs_file_name (str): GS file name

        Example:
            >>> from to_data_library.data import transfer
            >>> client = transfer.Client(project='my-project-id')
            >>> client.s3_to_gs(aws_session,
            >>>                 s3_bucket_name='my-s3-bucket_name',
            >>>                 s3_object_name='my-s3-file-prefix',
            >>>                 gs_bucket_name='my-gs-bucket-name',
            >>>                 gs_file_name='gs_file_name')
        """

        # Retrieve the file(s) from S3 matching to the object
        logs.client.logger.info('Finding files in S3 bucket')

        s3_files = self._get_keys_in_s3_bucket(aws_session=aws_session,
                                               bucket_name=s3_bucket_name,
                                               prefix_name=s3_object_name)

        logs.client.logger.info(f'Found {str(s3_files)} files in S3')

        # For every key found in s3, download to local and then upload to desired GS bucket.
        s3_client = s3.Client(aws_session)
        gs_client = gs.Client(self.project)
        for s3_file in s3_files:
            s3_client.download(s3_bucket_name, s3_file)

            gs_file_name = (gs_file_name if gs_file_name is not None else s3_file) \
                if len(s3_files) == 1 else s3_file
            gs_client.upload(os.path.basename(s3_file),
                             gs_bucket_name, gs_file_name)

    def _get_keys_in_s3_bucket(self, aws_session, bucket_name, prefix_name):
        """Generate a list of keys for objects in an s3 bucket.
        Paginates the list_objects_v2 method to overcome 1000 key limit.

        Args:
            aws_session: authenticated AWS session.
            bucket_name (str): Name of S3 bucket
            prefix_name (str): Prefix to search bucket for keys

        Returns:
            list: List of keys in that bucket that match the desired prefix
        """
        s3_client_boto = aws_session.client('s3')
        s3_files = []
        paginator = s3_client_boto.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix_name)
        for page in pages:
            for obj in page['Contents']:
                s3_files.append(obj.get('key'))
        return s3_files

    def s3_to_bq(self, aws_session, bucket_name, object_name,
                 bq_table, write_preference, auto_detect=True, separator=',',
                 skip_leading_rows=True, schema=None, partition_date=None):
        """
        Exports S3 file to BigQuery table

        Args:
          aws_session: authenticated AWS session.
          bucket_name (str): s3 bucket name
          object_name (str): s3 object name to copy
          bq_table (str): The BigQuery table. For example: ``my-project-id.my-dataset.my-table``
          write_preference (str): The option to specify what action to take when you load data from a source file.
            Value can be on of
                                            ``'empty'``: Writes the data only if the table is empty.
                                            ``'append'``: Appends the data to the end of the table.
                                            ``'truncate'``: Erases all existing data in a table before writing the new
                                            data.
          auto_detect (boolean, Optional):  True if the schema should automatically be detected otherwise False.
            Defaults to `True`.
          separator (str, optional): The separator. Defaults to `,`.
          skip_leading_rows (boolean, Optional):  True to skip the first row of the file otherwise False. Defaults to
            `True`.
          schema (list of tuples, optional): The BigQuery table schema. For example: ``[('first_field','STRING'),
          ('second_field', 'STRING')]``
          partition_date (str, Optional): The ingestion date for partitioned BigQuery table. For example: ``20210101``.
          The partition field name will be __PARTITIONTIME

        Example:
            >>> from to_data_library.data import transfer
            >>> client = transfer.Client(project='my-project-id')
            >>> client.s3_to_bq(aws_connection,
            >>>                 bucket_name='my-s3-bucket_name',
            >>>                 object_name='my-s3-object-name',
            >>>                 bq_table='my-project-id.my-dataset.my-table')
        """

        # Download S3 file to local
        s3_client = s3.Client(aws_session)
        s3_client.download(bucket_name, object_name,
                           os.path.join('/tmp/', object_name))

        logs.client.logger.info('Loading S3 file to BigQuery table')
        bq_client = bq.Client(bq_table.split('.')[0])
        bq_client.upload_table(
            file_path=os.path.join('/tmp/', object_name),
            table=bq_table,
            write_preference=write_preference,
            separator=separator,
            auto_detect=auto_detect,
            skip_leading_rows=skip_leading_rows,
            schema=schema,
            partition_date=partition_date
        )
        logs.client.logger.info('Loading completed')
