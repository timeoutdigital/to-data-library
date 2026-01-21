import datetime
import os
import re
from io import BytesIO
from pathlib import Path
from typing import Tuple

import pandas as pd
from google.api_core import exceptions
from google.cloud import bigquery, storage

from to_data_library.data import bq, ftp, gs, logs, s3
from to_data_library.data._helper import (gcs_bucket_name, gcs_filename,
                                          gcs_metadata, gcs_prefix,
                                          get_bq_write_disposition,
                                          merge_files)


class Client:
    """
    Client to bundle transfers from a source to destination.

    Args:
        project (str): The Project ID for the project which the client acts on behalf of.
    """

    def __init__(self, project, impersonated_credentials=None):
        self.project = project
        self.impersonated_credentials = impersonated_credentials

    def bq_to_gs(self, table, bucket_name, separator=',', print_header=True, compress=False):
        """Extract BigQuery table into the GoogleStorage

        Args:
            table (str):  The BigQuery table name. For example: ``my-project-id.you-dataset.my-table``
            bucket_name (str):  The name of the bucket in GoogleStorage (no 'gs://' prefix).
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
        logs.client.logger.info(
            'Extracting from {table} to gs://{bucket_name}/{table_id}_*'.format(
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

    def gs_to_bq(
            self,
            business_type,
            source_type,
            source,
            ingestion_type,
            etl_datetime_utc,
            dimension,
            table,
            file_number=None,
            write_preference='append',
            auto_detect=True,
            max_bad_records=0,
            schema_update_options=None,
            schema=None,
            data_date=None,
            partition_field=None,
            job_config_kwargs=None,
            transform_function=None
            ) -> Tuple[bool, str]:
        """
        - Loads file(s) from Google storage bucket to BigQuery table
        - Uses the standard naming conventions for bucket, prefix and file name
          as per documentation here:
          https://timeoutgroup.atlassian.net/wiki/spaces/TD/pages/3824189448/ELT+Process
        - Adds etl_datetime_utc column to the dataframe before loading to BigQuery
        - If multiple files are found, they are all loaded into BigQuery

        Args:
            business_type (str): The business type of the data being ingested. Generally 'markets' or 'web'.
            source_type (str): The source type of the data being ingested. E.g. 'pos', 'user', 'db', 'tracking', 'ads'
            source (str): The source of the data being ingested. E.g. 'mariadb_datacafe', 'tenzo', 'facebook'
            ingestion_type (str): The type of ingestion. Either 'batch' or 'stream'.
            etl_datetime_utc (str): load datetime string to use in the path
            dimension (str): The dimension of the data being ingested. E.g. 'audience', 'sales'
            table (str): The BigQuery table name. For example: ``project.dataset.table``.
            write_preference (str): The option to specify what action to take when you load data from a source file.
              Value can be one of
                                              ``'empty'``: Writes the data only if the table is empty.
                                              ``'append'``: Appends the data to the end of the table.
                                              ``'truncate'``: Erases all existing data in a table before writing the
                                                new data.
            auto_detect (boolean, Optional):  True if the schema should automatically be detected otherwise False.
              Defaults to :data:`True`.
            max_bad_records (int, Optional): The maximum number of rows with errors allowed when loading data.
            schema_update_options (List[bigquery.SchemaUpdateOption], Optional): Specifies the schema update options
              to use when loading data into a table. The schema_update_options property
              allows the schema of the destination table to be updated as a side effect of loading
              new data. The schema_update_options can only be used when write_preference is set to 'append' or
              'truncate'.
            schema (List[bigquery.SchemaField], Optional): The BigQuery table schema. Can be a partial schema.
            data_date (str, Optional): effective date of the data if one exists
              - this refers to the 'date' part of the prefix
            partition_field (str, Optional): The field on which the destination table is partitioned. The field must be
              a top-level TIMESTAMP or DATE field. Must be used in conjuction with partitioned_date.
              Here partitioned_date will be used to update or alter the table using the partition
            job_config_kwargs (dict, Optional): Any additional properties to set for the job config.
            transform_function (Callable, Optional): A function that takes a pandas DataFrame as input and
              returns a transformed DataFrame.

        Returns:
            (bool, str): Tuple with success status and message
        """

        bucket_name = gcs_bucket_name(
            project=self.project,
            business_type=business_type,
            source_type=source_type,
        )
        prefix = gcs_prefix(source, ingestion_type, etl_datetime_utc, data_date)
        file_name = gcs_filename(source, dimension, etl_datetime_utc, data_date, file_number)

        if job_config_kwargs is None:
            job_config_kwargs = {}
        if max_bad_records > 0:
            job_config_kwargs['max_bad_records'] = max_bad_records
        if schema_update_options:
            job_config_kwargs['schema_update_options'] = schema_update_options

        gs_client = gs.Client(self.project, impersonated_credentials=self.impersonated_credentials)
        bq_client = bq.Client(self.project, impersonated_credentials=self.impersonated_credentials)

        # Get all blobs in the bucket matching the file name
        blobs = list(gs_client.get_blobs(bucket_name, prefix=prefix))
        blobs = [blob for blob in blobs if blob.name.startswith(f"{prefix}/{file_name}")]

        logs.client.logger.info(f"Found {len(blobs)} files in gs://{bucket_name}/{prefix}/ matching {file_name}")

        for blob in blobs:
            logs.client.logger.info(f"Loading file {blob.name}...")
            try:
                # Load as dataframe
                df = self.load_gcs_file_as_dataframe(blob)
            except Exception as e:
                logs.client.logger.error(f"Error loading GCS file {blob.name} as dataframe: {e}")
                return False, str(e)
            # Add metadata: etl_datetime_utc column
            df['etl_datetime_utc'] = etl_datetime_utc

            if transform_function:
                try:
                    df = transform_function(df)
                except Exception as e:
                    logs.client.logger.error(f"Error applying transform function to dataframe from {blob.name}: {e}")
                    return False, str(e)

            dfs = []
            if partition_field:

                # Ensure partition field is in the dataframe
                if partition_field not in df.columns:
                    logs.client.logger.error(
                        f"Partition field {partition_field} not found in dataframe columns")
                    return False, f"Partition field {partition_field} not found in dataframe columns"

                # Split df into separate dataframes for each partition date
                partition_dates = df[partition_field].unique()
                multiple_partitions = len(partition_dates) > 1
                logs.client.logger.info(
                    f"Found {len(partition_dates)} partition{'s' if multiple_partitions else ''} in the data."
                    )
                if multiple_partitions:
                    logs.client.logger.info("Splitting dataframe into separate partitions for loading.")
                for date in partition_dates:
                    partition_df = df[df[partition_field] == date]
                    try:
                        part_date = date.strftime('%Y%m%d')
                    except Exception as e:
                        logs.client.logger.error(f"Error formatting partition date {date}: {e}")
                        return (
                            False,
                            f"Error formatting partition date {date}."
                            f" Ensure partition field {partition_field} contains all valid date or datetime values."
                        )
                    dfs.append({'df': partition_df, 'partition_date': part_date})
            else:
                dfs.append({'df': df, 'partition_date': None})

            try:
                for df in dfs:
                    bq_client.load_table_from_dataframe(
                        df['df'],
                        table,
                        write_preference,
                        auto_detect,
                        schema,
                        df['partition_date'],
                        partition_field,
                        job_config_kwargs
                        )
            except Exception as e:
                logs.client.logger.error(f"Error loading dataframe to BQ table {table}: {e}")
                return False, str(e)

        return True, f'Successfully loaded files from gs://{bucket_name}/{prefix}/{file_name} to {table}'

    def gs_parquet_to_bq(self, gs_uris, table, write_preference, auto_detect=True,
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
            source_format=bigquery.SourceFormat.PARQUET,
            autodetect=auto_detect if not schema else False,
            write_disposition=get_bq_write_disposition(write_preference),
            max_bad_records=max_bad_records
        )

        if partition_date:
            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY)
            table_id += '${}'.format(partition_date)

        bq_client = bq.Client(project=project,
                              impersonated_credentials=self.impersonated_credentials)
        try:
            bq_client.create_dataset(dataset_id)
        except exceptions.Conflict:
            logs.client.logger.info(
                'Dataset {} Already exists'.format(dataset_id))

        if schema:
            if isinstance(schema[0], bigquery.SchemaField):
                job_config.schema = schema
            else:
                job_config.schema = [bigquery.SchemaField(field[0], field[1]) for field in schema]

        logs.client.logger.info(
            'Loading BigQuery table {} from {}'.format(table, gs_uris))
        bq_client.load_table_from_uris(
            gs_uris=gs_uris, table_ref=table_ref, job_config=job_config)

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
            skip_leading_rows (boolean, Optional): True to skip the first row of the file otherwise False.
                Defaults to :data:`True`.
            bq_table_schema (tuple, Optional): The BigQuery table schema. For example: ``(('first_field','STRING'),
            ('second_field','STRING'))``
            partition_date (str, Optional): The ingestion date for partitioned BigQuery table.
                For example: ``20210101``.
            The partition field name will be __PARTITIONTIME

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
        bq_client = bq.Client(project=self.project,
                              impersonated_credentials=self.impersonated_credentials)
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
            separator (:obj:`str`, optional): The separator. Defaults to :data:`,`.\n
            print_header (boolean, Optional):  True to write header for the CSV file, otherwise False.
            Defaults to : data:`True`.

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
        bq_client = bq.Client(project=self.project,
                              impersonated_credentials=self.impersonated_credentials)
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
        s3_bucket (str): s3 bucket name

        Example:
            >>> from to_data_library.data import transfer
            >>> client = transfer.Client(project='my-project-id')
            >>> client.gs_to_s3(aws_session,
            >>>                 gs_uri='gs://my-bucket-name/my-filename',
            >>>                 s3_bucket='bucket_name')
        """

        local_file = os.path.basename(gs_uri)
        gs_client = gs.Client(self.project,
                              impersonated_credentials=self.impersonated_credentials)
        gs_client.download(gs_uri, local_file)

        s3_client = s3.Client(aws_session)
        s3_client.upload(local_file,
                         s3_bucket)

    def s3_to_gs(
            self,
            aws_session,
            s3_bucket_name,
            s3_object_or_prefix_name,
            business_type,
            source_type,
            source,
            dimension,
            repo_name='Unknown',
            ingestion_type='batch',
            file_number='000',
            wildcard=None,
            data_date=None,
            etl_datetime_utc=None
            ) -> Tuple[bool, str]:
        """
        - Exports file(s) from S3 bucket to Google storage bucket
        - Enforces the use of the standard naming conventions for bucket, prefix, file name and metadata
          as per documentation here:
          https://timeoutgroup.atlassian.net/wiki/spaces/TD/pages/3824189448/ELT+Process
        - Added threading to speed up execution

        Args:
            aws_session: authenticated AWS session.
            s3_bucket_name (str): s3 bucket name
            s3_object_or_prefix_name (str): s3 object name or prefix to match multiple files to copy,
            business_type (str): The business type of the data being ingested. Generally 'markets' or 'web'.
            source_type (str): The source type of the data being ingested. E.g. 'pos', 'user', 'db', 'tracking', 'ads'
            source (str): The source of the data being ingested. E.g. 'mariadb_datacafe', 'tenzo', 'facebook'
            dimension (str): The dimension of the data being ingested. E.g. 'audience', 'sales'
            repo_name (str, Optional): The name of the repo that is running the ingestion. Defaults to 'Unknown'.
            ingestion_type (str, Optional): The type of ingestion. Either 'batch' or 'stream'. Defaults to 'batch'.
            file_number (str, Optional): The file number. Defaults to '000'.
            wildcard (str): regex wildcard (default '.*')
            data_date (str): effective date of the data set if one exists
              - this determines the 'date' part of the prefix
            etl_datetime_utc (str): load datetime string to use in the path and file name
        Returns:
            (bool, str): Tuple with success status and message

        Example:
            >>> from to_data_library.data import transfer
            >>> client = transfer.Client(project='my-project-id')
            >>> success, message = client.s3_to_gs(aws_session,
            >>>                                 s3_bucket_name='my-s3-bucket',
            >>>                                 s3_object_or_prefix_name='my-s3-object-or-prefix',
            >>>                                 project='tog-dev-dt-lnd',
            >>>                                 business_type='markets',
            >>>                                 source_type='pos',
            >>>                                 source='tenzo',
            >>>                                 dimension='sales')
        """

        # Quality Checks
        # Ensure no underscores in source or dimension
        for element in [source, dimension]:
            if '_' in element:
                return False, f"Error: {element} must not contain underscores."

        if not etl_datetime_utc:
            etl_datetime_utc = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

        # Build the GS bucket name, prefix and metadata
        gs_bucket_name = gcs_bucket_name(
            project=self.project,
            business_type=business_type,
            source_type=source_type,
        )
        gs_prefix = gcs_prefix(source, ingestion_type, etl_datetime_utc, data_date)

        metadata = gcs_metadata(s3_bucket_name, s3_object_or_prefix_name, etl_datetime_utc, repo_name)

        # Retrieve the file(s) from S3 matching to the object
        logs.client.logger.info('Finding files in S3 bucket')

        if not wildcard:
            wildcard = '.*'

        s3_files = self._get_keys_in_s3_bucket(
            aws_session=aws_session,
            bucket_name=s3_bucket_name,
            prefix_name=s3_object_or_prefix_name,
            wildcard=wildcard)

        logs.client.logger.info(f'Found {len(s3_files)} files in S3')

        # Get S3 and GS clients
        s3_client = s3.Client(aws_session)
        gs_client = gs.Client(self.project, impersonated_credentials=self.impersonated_credentials)

        # For every key found in s3, download to local and then upload to desired GS bucket.
        for file_number, s3_file in enumerate(s3_files, start=0):
            file_number = f"{file_number:03d}"  # zero-padded to 3 digits

            # Try to download the file to local
            try:
                local_file = s3_client.download(s3_bucket_name, s3_file)
                logs.client.logger.info(f'Successfully downloaded {local_file} to local')
            except Exception as e:
                logs.client.logger.error(f"Failed to download {local_file} to local: {e}")
                return False, str(e)

            # Try to upload file from local to GCS.
            try:
                s3_file_extension = '.'.join(Path(s3_file).suffixes).lstrip('.')
                gs_file_name = gcs_filename(
                    source,
                    dimension,
                    etl_datetime_utc,
                    data_date,
                    file_number,
                    s3_file_extension
                )
                gs_file_path = '/'.join([gs_prefix, gs_file_name])

                gs_client.upload(
                    source_file_name=local_file,
                    bucket_name=gs_bucket_name,
                    blob_name=gs_file_path,
                    metadata=metadata
                )
                logs.client.logger.info(
                    f'Successfully uploaded {local_file} to {gs_bucket_name}/{gs_file_path}')
            except Exception as e:
                logs.client.logger.error(
                    f"Failed to upload {local_file} to {gs_bucket_name}/{gs_file_name}: {e}")
                return False, str(e)
            finally:
                if os.path.exists(local_file):
                    os.remove(local_file)
                    logs.client.logger.info(f'Deleted local file {local_file}')

        return True, f'Successfully transferred {len(s3_files)} files from S3 to GS'

    def _get_keys_in_s3_bucket(self, aws_session, bucket_name, prefix_name, wildcard='.*'):
        """Generate a list of keys for objects in an s3 bucket.
        Paginates the list_objects_v2 method to overcome 1000 key limit.

        Args:
            aws_session: authenticated AWS session.
            bucket_name (str): Name of S3 bucket
            prefix_name (str): Prefix to search bucket for keys
            wildcard (str): Option wildcard for filtering

        Returns:
            list: List of keys in that bucket that match the desired prefix
        """
        s3_client_boto = aws_session.client('s3')
        s3_files = []
        paginator = s3_client_boto.get_paginator('list_objects_v2')

        regex = re.compile(wildcard)

        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix_name)
        for page in pages:
            for obj in page.get('Contents', []):
                key = obj['Key']
                if not key.endswith('/') and re.match(regex, key):
                    s3_files.append(obj.get('Key'))

        return s3_files

    def load_gcs_file_as_dataframe(self, blob) -> pd.DataFrame:
        """
        Loads a GCS blob into a pandas dataframe
        Args:
            blob: The GCS blob object
        Returns:
            pd.DataFrame: The pandas dataframe
        Example:
            >>> from to_data_library.data import transfer
            >>> client = transfer.Client(project='my-project-id')
            >>> gs_client = gs.Client(project='my-project-id')
            >>> bucket = gs_client.bucket('my-bucket-name')
            >>> blob = bucket.blob('my-blob-name')
            >>> df = client.load_gcs_file_as_dataframe(blob)
        """
        file_type = blob.name.split('.')[-1]
        data = BytesIO(blob.download_as_bytes())
        if file_type == 'csv':
            return pd.read_csv(data)
        elif file_type == 'json':
            return pd.read_json(data, lines=True)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
