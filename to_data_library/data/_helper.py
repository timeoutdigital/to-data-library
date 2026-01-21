from datetime import datetime

from google.cloud import bigquery


def merge_files(files_path, output_file_path=None):

    output = 'merged.csv'

    with open(output_file_path if output_file_path else output, 'w') as outfile:
        for file_path in files_path:
            with open(file_path) as infile:
                outfile.write(infile.read())

    return output


def get_bq_write_disposition(write_preference):
    """Convert write_preference string to BigQuery WriteDisposition values

    Args:
        write_preference (str): The write preference string which should be 'truncate', 'append' or 'empty'

    Returns:
        bigquery.WriteDisposition: The BigQuery WriteDisposition value

    """

    disposition = {
        'truncate': bigquery.WriteDisposition.WRITE_TRUNCATE,
        'append': bigquery.WriteDisposition.WRITE_APPEND,
        'empty': bigquery.WriteDisposition.WRITE_EMPTY
    }

    return disposition.get(write_preference, None)


def gcs_prefix(source, ingestion_type, etl_datetime_utc, date=None) -> str:
    """
    Builds a Google Cloud Storage prefix string with validation and consistent datetime formatting.

    Args:
        source (str): The source of the data, e.g. 'mariadb_datacafe', 'tenzo', 'facebook'.
        ingestion_type (str): The type of ingestion, e.g. 'batch' or 'stream'.
        etl_datetime_utc (datetime): The ETL datetime in UTC.
        date (datetime or str, optional): The effective data date. Can be a datetime object or string. Defaults to None.

    Returns:
        str: The GCS prefix in the format '{source}/{ingestion_type}/[date/]{etl_datetime}/'.

    Raises:
        ValueError: If required arguments are missing or invalid.
    """
    if not source or not isinstance(source, str):
        raise ValueError("source must be a non-empty string")
    if not ingestion_type or not isinstance(ingestion_type, str):
        raise ValueError("ingestion_type must be a non-empty string")
    if not etl_datetime_utc:
        raise ValueError("etl_datetime_utc is required")
    if not hasattr(etl_datetime_utc, 'strftime'):
        raise ValueError("etl_datetime_utc must be a datetime object")
    parts = [source, ingestion_type]
    if date:
        if hasattr(date, 'strftime'):
            parts.append(date.strftime('%Y_%m_%d'))
        elif isinstance(date, str):
            try:
                # Try to parse string to datetime
                parsed_date = datetime.strptime(date, '%Y-%m-%d')
                parts.append(parsed_date.strftime('%Y_%m_%d'))
            except Exception:
                raise ValueError(
                    "date must be a datetime object or string in 'YYYY-MM-DD' format"
                )
        else:
            raise ValueError("date must be a datetime object or string")
    parts.append(etl_datetime_utc.strftime('%Y_%m_%d_%H_%M_%S'))
    return '/'.join(parts)


def gcs_filename(source, dimension, etl_datetime_utc, data_date=None, file_number=None, file_extension=None) -> str:
    """
    Builds a Google Cloud Storage file name string with validation and consistent datetime formatting.

    Args:
        source (str): The source of the data, e.g. 'mariadb_datacafe', 'tenzo', 'facebook'.
        dimension (str): The dimension of the data, e.g. 'audience', 'sales'.
        etl_datetime_utc (datetime): The ETL datetime in UTC.
        data_date (datetime or str, optional): The effective data date. Can be a datetime object or string.
            Defaults to None.
        file_number (str, optional): The file number, e.g. '001'. Defaults to None.
        file_extension (str, optional): The file extension without the leading dot,
            e.g. 'ndjson', 'csv'. Defaults to None.

    Returns:
        str: The GCS file name in the format '{source}_{dimension}_[date]_etl_datetime_file_number.extension'.

    Raises:
        ValueError: If required arguments are missing or invalid.
    """
    if not source or not isinstance(source, str):
        raise ValueError("source must be a non-empty string")
    if not dimension or not isinstance(dimension, str):
        raise ValueError("dimension must be a non-empty string")
    if not etl_datetime_utc:
        raise ValueError("etl_datetime_utc is required")
    if not hasattr(etl_datetime_utc, 'strftime'):
        raise ValueError("etl_datetime_utc must be a datetime object")
    parts = [source, dimension]
    if data_date:
        if hasattr(data_date, 'strftime'):
            parts.append(data_date.strftime('%Y_%m_%d'))
        elif isinstance(data_date, str):
            try:
                parsed_date = datetime.strptime(data_date, '%Y-%m-%d')
                parts.append(parsed_date.strftime('%Y_%m_%d'))
            except Exception:
                raise ValueError(
                    "data_date must be a datetime object or string in 'YYYY-MM-DD' format"
                )
        else:
            raise ValueError("data_date must be a datetime object or string")
    parts.append(etl_datetime_utc.strftime('%Y_%m_%d_%H_%M_%S'))
    if file_number:
        parts.append(str(file_number))
    file_name = '_'.join(parts)
    if file_extension:
        file_name = f"{file_name}.{file_extension}"
    return file_name


def gcs_metadata(s3_bucket_name, s3_object_name, etl_datetime_utc, repo_name) -> dict:
    """
    Builds the gs metadata based on s3 bucket name, s3 object name and etl datetime.

    Args:
        s3_bucket_name (str): s3 bucket name
        s3_object_name (str): s3 object name
        etl_datetime_utc (str): load datetime string to use in the path
        repo_name (str): The name of the repo that is running the ingestion

    Returns:
        dict: The gs metadata
    """
    return {
        's3_bucket_name': s3_bucket_name,
        's3_object_name': s3_object_name,
        'etl_datetime_utc': etl_datetime_utc,
        'repo_name': repo_name
    }


def gcs_bucket_name(project: str, business_type: str, source_type: str) -> str:
    """
    Builds a Google Cloud Storage bucket name using the standard naming convention.

    The bucket name is composed as:
        '{gcp_project}-{business_type}-{source_type}'

    Args:
        project (str):
            Google Cloud project ID that owns the bucket
            (e.g. 'tog-dev-dt-lnd', 'tog-prod-dt-lnd').
        business_type (str):
            The business domain of the data being ingested
            (e.g. 'markets', 'web').
        source_type (str):
            The source category of the data being ingested
            (e.g. 'pos', 'user', 'db', 'tracking', 'ads').

    Returns:
        str:
            The Google Cloud Storage bucket name (without 'gs://').

    Raises:
        ValueError:
            If any input is missing or invalid.
    """
    if not all(isinstance(x, str) and x for x in [project, business_type, source_type]):
        raise ValueError("project, business_type and source_type must be non-empty strings")
    return f"{project}-{business_type}-{source_type}"
