from google.cloud import storage


class Client:
    """
    Client to bundle Google Storage functionality.

    Args:
        project (str): The Project ID for the project which the client acts on behalf of.
    """

    def __init__(self, project):
        self.project = project
        self.storage_client = storage.Client(project=self.project)

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
            
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(source_file_name)

    def create_bucket(self, bucket_name):
        """create a bucket in Google Storage.

        Args:
            bucket_name (str):  The Google Storage bucket name.
        """

        self.storage_client.create_bucket(bucket_name, location='EU')
