import csv
import unittest
import unittest.mock
from unittest.mock import Mock, patch

from google.cloud import bigquery

# from tests.setup import setup
from to_data_library.data import bq

# def setUpModule():
#     setup.create_bq_table()
#     setup.create_bucket()


# def tearDownModule():
#     setup.delete_bq_dataset()
#     setup.delete_bucket()
#     setup.cleanup()


class TestBQ(unittest.TestCase):

    # def __init__(self, *args, **kwargs):
    #     super(TestBQ, self).__init__(*args, **kwargs)
    #     self.setup = setup

    @patch('google.cloud.storage.Client')
    @patch('google.cloud.bigquery.Client')
    def test_create_tmp_bucket_in_gcs(self, mock_bigquery, mock_storage):
        mock_storage_client = mock_storage.return_value

        bq_client = bq.Client(project='fake_project')
        bq_client._create_tmp_bucket_in_gcs(mock_storage_client)

        mock_storage_client.create_bucket.assert_called_once()

    @patch('google.cloud.storage.Client')
    @patch('google.cloud.bigquery.Client')
    @patch('to_data_library.data.transfer.Client')
    def test_download_table(self, mock_transfer, mock_bigquery, mock_storage):
        mock_storage_client = mock_storage.return_value
        mock_transfer_client = mock_transfer.return_value
        mock_bucket = Mock()
        mock_storage_client.create_bucket.return_value = mock_bucket
        mock_bucket.name = 'random_uuid'
        mock_bucket = Mock()
        mock_bucket.name.return_value = 'tmp'
        mock_storage_client.bucket.return_value = mock_bucket
        mock_blob = Mock()
        mock_bucket.blob.return_value = mock_blob

        bq_client = bq.Client(project=self.setup.project)
        bq_client.download_table(
            table='{}.{}.{}'.format('fake_project', 'fake_data_set_id', 'fake_table_id')
        )

        mock_storage.assert_called_once_with(project=self.setup.project)
        mock_storage_client.list_blobs.assert_called_once_with('random_uuid')
        mock_transfer_client.bq_to_gs.assert_called_once_with('fake_project.fake_data_set_id.fake_table_id',
                                                              'random_uuid',
                                                              separator=',',
                                                              print_header=True)

    def test_upload_table(self):
        # test upload BQ table

        bq_client = bq.Client(project=self.setup.project)
        bq_client.upload_table(
            file_path='tests/data/sample.csv',
            table='{}.{}.{}'.format(self.setup.project, self.setup.dataset_id, 'uploaded_actors'),
            write_preference='truncate'
        )

        # creating list based on source file values
        with open('tests/data/sample.csv', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            storage_keys = []
            for row in reader:
                storage_keys.append(
                    str(row['profile_id']) + row['first_name'] + row['last_name']
                )

        # creating list based on destination big query table values
        bq_keys = []
        bigquery_client = bigquery.Client(project=self.setup.project)
        job = bigquery_client.query(
            'SELECT profile_id, first_name, last_name from {}.{}'.format(
                self.setup.dataset_id,
                'uploaded_actors'
            )
        )
        for row in job.result():
            bq_keys.append('{}{}{}'.format(
                row.profile_id,
                '' if row.first_name is None else row.first_name,
                '' if row.last_name is None else row.last_name,
            ))

        self.assertEqual(storage_keys, bq_keys)

    def test_upload_table_partitioned_default(self):
        # test deafult date type for partitioned table

        with unittest.mock.patch('to_data_library.data.bq.bigquery') as bq_mock:
            with unittest.mock.patch('to_data_library.data.bq.default') as default_mock:
                with unittest.mock.patch('to_data_library.data.bq.open') as _:
                    default_mock.return_value = 'nothing', 'here'
                    client = bq.Client('foo')
                    client.upload_table(
                        table='some.table.foo',
                        file_path='notchecked',
                        write_preference='notchecked',
                        partition_date="bar",
                        partition_field="notused"
                    )
                    bq_mock.TimePartitioning.assert_called_with(type_=bq_mock.TimePartitioningType.DAY)
                    expected_table_id = 'foo$bar'
                    bq_mock.TableReference.assert_called_with(unittest.mock.ANY, table_id=expected_table_id)

    def test_upload_table_partitioned_field(self):
        # test field partitioned table

        with unittest.mock.patch('to_data_library.data.bq.bigquery') as bq_mock:
            with unittest.mock.patch('to_data_library.data.bq.default') as default_mock:
                with unittest.mock.patch('to_data_library.data.bq.open') as _:
                    default_mock.return_value = 'nothing', 'here'
                    client = bq.Client('foo')
                    client.upload_table(
                        table='some.table.test',
                        file_path='notchecked',
                        write_preference='notchecked',
                        partition_field="foo"
                    )
                    bq_mock.TimePartitioning.assert_called_with(type_=bq_mock.TimePartitioningType.DAY, field="foo")
                    expected_table_id = "test"
                    bq_mock.TableReference.assert_called_with(unittest.mock.ANY, table_id=expected_table_id)

    def test_run_query(self):

        # creating list based on run_query return's results
        bq_client = bq.Client(project=self.setup.project)
        results = bq_client.run_query(
            query='SELECT * FROM {}.{} where profile_id={{{{id}}}}'.format(self.setup.dataset_id, self.setup.table_id),
            params={'id': 1}

        )
        keys = []
        for row in results:
            keys.append('{}{}{}'.format(
                row.profile_id,
                '' if row.first_name is None else row.first_name,
                '' if row.last_name is None else row.last_name,
            ))

        # creating list based on source big query table values from Googl Client
        bigquery_client = bigquery.Client(project=self.setup.project)
        job = bigquery_client.query(
            query='SELECT profile_id, first_name, last_name from {}.{} where profile_id=@id'.format(
                self.setup.dataset_id,
                self.setup.table_id
            ),
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter('id', 'INT64', 1)]
            )
        )
        bq_keys = []
        for row in job.result():
            bq_keys.append('{}{}{}'.format(
                row.profile_id,
                '' if row.first_name is None else row.first_name,
                '' if row.last_name is None else row.last_name,
            ))

        self.assertEqual(keys, bq_keys)

    @unittest.expectedFailure
    def test_create_table_with_no_schema(self):
        bq_client = bq.Client('fake_project_name')
        bq_client.create_table(
            table='{}.{}.{}'.format('fake_project_id', 'fake_dataset_id', 'fake_table_id')
        )

    @patch('google.cloud.bigquery.Client')
    @patch('google.cloud.bigquery.Table')
    def test_create_table_with_schema_fields(self, mock_bq_table, mock_bigquery):
        bq_client = bq.Client(project='fake_project')
        bq_client.create_table(
            table='{}.{}.{}'.format('fake_project', 'fake_dataset_id', 'venue'),
            schema_fields=(('venue_id', 'STRING', 'REQUIRED'), ('name', 'STRING', 'REQUIRED'))
        )
        mock_bq_table.assert_called_with('fake_project.fake_dataset_id.venue',
                                         schema=[bigquery.SchemaField('venue_id', 'STRING', 'REQUIRED', None,
                                                                      None, (), None),
                                                 bigquery.SchemaField('name', 'STRING', 'REQUIRED', None,
                                                                      None, (), None)])
        mock_bq_table_instance = mock_bq_table.return_value

        mock_bigquery.return_value.create_table.assert_called_with(mock_bq_table_instance)

    @patch('google.cloud.bigquery.Client')
    @patch('google.cloud.bigquery.Table')
    def test_create_table_with_schema_file_name(self, mock_bq_table, mock_bigquery):
        bq_client = bq.Client(project='fake_project')
        bq_client.create_table(
            table='{}.{}.{}'.format('fake_project', 'fake_dataset_id', 'venue2'),
            schema_file_name='tests/data/schema.csv'
        )

        mock_bq_table.assert_called_with('fake_project.fake_dataset_id.venue2',
                                         schema=[bigquery.SchemaField('venue_id', 'STRING', 'REQUIRED', None,
                                                                      None, (), None),
                                                 bigquery.SchemaField('name', 'STRING', 'REQUIRED', None,
                                                                      None, (), None),
                                                 bigquery.SchemaField('address', 'STRING', 'REQUIRED', None,
                                                                      None, (), None)])
        mock_bq_table_instance = mock_bq_table.return_value

        mock_bigquery.return_value.create_table.assert_called_with(mock_bq_table_instance)
