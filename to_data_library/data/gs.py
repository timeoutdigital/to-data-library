from google.cloud import storage


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
        bucket_rename = bucket_name.replace('gs://','')
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
        bucket_rename = bucket_name.replace('gs://','')
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
