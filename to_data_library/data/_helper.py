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
