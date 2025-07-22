import gzip
import json
from io import BytesIO, TextIOWrapper

import ndjson
from google.cloud import storage

from to_data_library.data import logs


class Client:
    """
    Client to bundle Google Storage functionality.

    Args:
        project (str): The Project ID for the project which the client acts on behalf of.
        impersonated_credentials (google.auth.impersonated_credentials) : The scoped
        impersonated credentials object that will be used to authenticate the client
    """

    def __init__(self, project, impersonated_credentials=None):
        self.project = project
        self.storage_client = storage.Client(project=self.project,
                                             credentials=impersonated_credentials)

    def download(self, gs_uri, destination_file_name=None):
        """Download from Google Storage to local.

        Args:
            gs_uri (str):  The Google Storage uri. For example: ``gs://my_bucket_name/my_filename``.
            destination_file_name (str):  The destination file name. For example: ``/some_path/some_file_name``.
            If not provided, destination_file_name will be name of file in GCS.
        """
        if not destination_file_name:
            destination_file_name = gs_uri.split('/')[-1]

        with open(destination_file_name, 'wb') as file_obj:
            self.storage_client.download_blob_to_file(gs_uri, file_obj)

    def upload(self, source_file_name, bucket_name, blob_name=None):
        """Upload from local to Google Storage.

        Args:
            source_file_name (str):  The source file name.
            bucket_name (str):  The Google Storage bucket name (no 'gs://' prefix).
            blob_name (str): The destination file name in the bucket, if not provided source file name will be used.
        """
        if not blob_name:
            blob_name = source_file_name.split('/')[-1]

        blob = self.storage_client.bucket(bucket_name).blob(blob_name)
        blob.upload_from_filename(source_file_name)

    def list_bucket_uris(self, bucket_name, file_type='csv', prefix=None):
        """Lists the files in a bucket

        Args:
            bucket_name (str): the bucket name (no 'gs://' prefix)
            file_type (str): the suffix for the files to list (default: csv)
            prefix (str): the folder path to the files

        Returns:
            list: The list of the contents
        """
        blobs = self.storage_client.list_blobs(bucket_name, prefix=prefix)
        gs_uris = [f"gs://{bucket_name}/{blob.name}" for blob in blobs if blob.name.endswith(file_type)]
        return gs_uris

    def create_bucket(self, bucket_name):
        """create a bucket in Google Storage.

        Args:
            bucket_name (str):  The Google Storage bucket name (no 'gs://' prefix).
        """

        self.storage_client.create_bucket(bucket_name, location='EU')

    def convert_json_array_to_ndjson(self, bucket_name, input_gz_file, output_file):
        """Converts a gzip json file to ndjson with minimal memory and storage usage.

        Args:
            bucket_name (str): the bucket name (no 'gs://' prefix)
            input_gz_file (str): the path and name of the GZIP file to be processed (format: bucket/path/to/file.gz)
            output_file (str): the path and name of the file to be created (format: bucket/path/to/file.ndjson)
        """
        input_blob = self.storage_client.bucket(bucket_name).blob(input_gz_file)
        target_blob = self.storage_client.bucket(bucket_name).blob(output_file)

        if target_blob.exists():
            target_blob.delete()

        input_stream = BytesIO(input_blob.download_as_bytes())
        output_stream = BytesIO()

        with gzip.GzipFile(fileobj=input_stream, mode='rb') as gz_file:
            with TextIOWrapper(gz_file, encoding='utf-8') as text_file:
                try:
                    # Try to treat file as a JSON array
                    json_data = json.load(text_file)
                    for json_obj in json_data:
                        output_stream.write(ndjson.dumps([json_obj]).encode('utf-8') + b'\n')
                except json.JSONDecodeError:
                    # Rewind stream and treat as line-delimited JSON (NDJSON)
                    input_stream.seek(0)
                    with gzip.GzipFile(fileobj=input_stream, mode='rb') as gz_file_reopened:
                        with TextIOWrapper(gz_file_reopened, encoding='utf-8') as line_file:
                            for line in line_file:
                                try:
                                    json_obj = json.loads(line)
                                    output_stream.write(ndjson.dumps([json_obj]).encode('utf-8') + b'\n')
                                except json.JSONDecodeError:
                                    self.logger.warning("Skipping malformed line in file: %s", input_gz_file)

        output_stream.seek(0)
        target_blob.upload_from_file(output_stream, content_type='application/x-ndjson')

        logs.client.logger.info(f"Converted and uploaded NDJSON file to: gs://{bucket_name}/{output_file}")
