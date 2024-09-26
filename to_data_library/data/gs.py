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
            bucket_name (str):  The Google Storage bucket name.
            blob_name (str): The destination file name in the bucket, if not provided source file name will be used.
        """
        if not blob_name:
            blob_name = source_file_name.split('/')[-1]

        # For the API to work, need to remove 'gs://'
        bucket_rename = bucket_name.replace('gs://', '')
        bucket = self.storage_client.bucket(bucket_rename)
        blob = bucket.blob(blob_name)

        blob.upload_from_filename(source_file_name)

    def list_bucket_uris(self, bucket_name, file_type='csv', prefix=None):
        """Lists the files in a bucket

        Args:
            bucket_name (str): the bucket name
            file_type (str): the prefix for the files to list (default: csv)
            prefix (str): the folder path to the files

        Returns:
            list: The list of the contents
        """

        # For the API to work, need to remove 'gs://'
        bucket_rename = bucket_name.replace('gs://', '')
        bucket = self.storage_client.bucket(bucket_rename)
        blobs = bucket.list_blobs(prefix=prefix)

        gs_uris = []

        for blob in blobs:
            if blob.name.endswith(file_type):
                uri = f"gs://{bucket_rename}/{blob.name}"
                gs_uris.append(uri)

        return gs_uris

    def create_bucket(self, bucket_name):
        """create a bucket in Google Storage.

        Args:
            bucket_name (str):  The Google Storage bucket name.
        """

        self.storage_client.create_bucket(bucket_name, location='EU')

    def convert_json_array_to_ndjson(self, bucket_name, input_gz_file, output_file):
        """Converts a gzip json file to ndjson with minimal memory and storage usage.

        Args:
            bucket_name (str): the bucket name
            input_gz_file (str): the path and name of the GZIP file to be processed (format: gs://path/to/file.gz)
            output_file (str): the path and name of the file to be created (format: gs://path/to/file.ndjson)
        """
        bucket_rename = bucket_name.replace('gs://', '')
        input_gz_file_rename = input_gz_file[len(bucket_name)+1:]
        output_file_rename = output_file[len(bucket_name)+1:]

        bucket = self.storage_client.bucket(bucket_rename)
        input_blob = bucket.blob(input_gz_file_rename)
        target_blob = bucket.blob(output_file_rename)

        if target_blob.exists():
            target_blob.delete()

        # Stream reading from the gzipped input file
        input_stream = BytesIO(input_blob.download_as_bytes())
        with gzip.GzipFile(fileobj=input_stream, mode='rb') as gz_file:
            # Wrap the gzipped file object in a text wrapper to read JSON line by line
            with TextIOWrapper(gz_file, encoding='utf-8') as text_file:
                # Use a generator to convert each JSON object to NDJSON and stream the output
                def json_to_ndjson_stream():
                    json_data = json.load(text_file)
                    for json_obj in json_data:
                        yield (ndjson.dumps([json_obj]) + '\n').encode('utf-8')

                # Upload the output to GCS in streaming mode
                target_blob.upload_from_file(BytesIO(b''.join(json_to_ndjson_stream())),
                                             content_type='application/x-ndjson')

        logs.client.logger.info(f"Converted and uploaded NDJSON file to: {output_file}")
