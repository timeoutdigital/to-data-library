import unittest
import unittest.mock
from unittest.mock import ANY, Mock, patch

import pandas as pd
from google.cloud import bigquery

from to_data_library.data import bq


class TestBQ(unittest.TestCase):
    @patch('google.cloud.storage.Client')
    @patch('google.cloud.bigquery.Client')
    @patch('to_data_library.data.bq.default')
    def test_create_tmp_bucket_in_gcs(self, mock_default, mock_bigquery, mock_storage):
        mock_default.return_value = 'first', 'second'
        mock_storage_client = mock_storage.return_value

        bq_client = bq.Client(project='fake_project')
        bq_client._create_tmp_bucket_in_gcs(mock_storage_client)

        mock_storage_client.create_bucket.assert_called_with(ANY, location='EU')

    @patch('google.cloud.storage.Client')
    @patch('google.cloud.bigquery.Client')
    @patch('to_data_library.data.transfer.Client')
    @patch('to_data_library.data.bq.default')
    def test_download_table(self, mock_default, mock_transfer, mock_bigquery, mock_storage):
        mock_default.return_value = 'first', 'second'
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

        bq_client = bq.Client(project='fake_project')
        bq_client.download_table(
            table='{}.{}.{}'.format('fake_project', 'fake_data_set_id', 'fake_table_id')
        )

        mock_storage.assert_called_once_with(project='fake_project')
        mock_storage_client.list_blobs.assert_called_once_with('random_uuid')
        mock_transfer_client.bq_to_gs.assert_called_once_with('fake_project.fake_data_set_id.fake_table_id',
                                                              'random_uuid',
                                                              separator=',',
                                                              print_header=True)

    @patch('google.cloud.bigquery.DatasetReference')
    @patch('google.cloud.bigquery.TableReference')
    @patch('google.cloud.bigquery.Client')
    @patch('google.cloud.bigquery.LoadJobConfig')
    @patch('to_data_library.data.bq.default')
    def test_upload_table(self, mock_default, mock_loadjobconfig,
                          mock_bigqueryclient, mock_tablereference, mock_datasetrefererence):
        mock_default.return_value = 'first', 'second'
        job_config = mock_loadjobconfig.return_value

        bq_client = bq.Client(project='fake_project')
        bq_client.upload_table(
            file_path='tests/data/sample.csv',
            table='{}.{}.{}'.format('fake_project', 'fake_data_set_id', 'uploaded_actors'),
            write_preference='truncate',
            max_bad_records=20
        )

        mock_datasetrefererence.assert_called_with(project='fake_project', dataset_id='fake_data_set_id')
        mock_loadjobconfig.assert_called_with(source_format='CSV',
                                              skip_leading_rows=1,
                                              autodetect=True,
                                              field_delimiter=',',
                                              write_disposition='WRITE_TRUNCATE',
                                              allow_quoted_newlines=True,
                                              max_bad_records=20)
        mock_tablereference.assert_called_with(ANY, table_id='uploaded_actors')

        # Check that schema is passed if provided to method
        bq_client.upload_table(
            file_path='tests/data/sample.csv',
            table='{}.{}.{}'.format('fake_project', 'fake_data_set_id', 'uploaded_actors'),
            write_preference='truncate',
            max_bad_records=20,
            schema=(('first_field', 'STRING'), ('second_field', 'STRING'))
        )
        self.assertEqual(job_config.schema,
                         [bigquery.SchemaField('first_field', 'STRING', 'NULLABLE', None, None, (), None),
                          bigquery.SchemaField('second_field', 'STRING', 'NULLABLE', None, None, (), None)])

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
                        partition_field="foo"
                    )
                    bq_mock.TimePartitioning.assert_called_with(type_=bq_mock.TimePartitioningType.DAY, field="foo")
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
                        partition_date="bar",
                        partition_field="foo"
                    )
                    bq_mock.TimePartitioning.assert_called_with(type_=bq_mock.TimePartitioningType.DAY, field="foo")
                    expected_table_id = "test$bar"
                    bq_mock.TableReference.assert_called_with(unittest.mock.ANY, table_id=expected_table_id)

    @patch('google.cloud.bigquery.DatasetReference')
    @patch('google.cloud.bigquery.TableReference')
    @patch('google.cloud.bigquery.Client')
    @patch('google.cloud.bigquery.LoadJobConfig')
    @patch('to_data_library.data.bq.default')
    def test_load_table_from_dataframe(
        self, mock_default, mock_loadjobconfig, _, mock_tablereference, mock_datasetrefererence
    ):
        mock_default.return_value = 'first', 'second'

        bq_client = bq.Client(project='fake_project')
        bq_client.load_table_from_dataframe(
            pd.read_csv('tests/data/sample.csv'),
            table='{}.{}.{}'.format('fake_project', 'fake_data_set_id', 'uploaded_actors'),
            write_preference='truncate',
        )

        mock_datasetrefererence.assert_called_with(project='fake_project', dataset_id='fake_data_set_id')
        mock_loadjobconfig.assert_called_with(autodetect=True, write_disposition='WRITE_TRUNCATE')
        mock_tablereference.assert_called_with(ANY, table_id='uploaded_actors')

    def test_load_table_from_dataframe_partitioned_default(self):
        # test default date type for partitioned table

        with unittest.mock.patch('to_data_library.data.bq.bigquery') as bq_mock:
            with unittest.mock.patch('to_data_library.data.bq.default') as default_mock:
                with unittest.mock.patch('to_data_library.data.bq.open') as _:
                    default_mock.return_value = 'nothing', 'here'
                    client = bq.Client('foo')
                    client.load_table_from_dataframe(
                        data_df=pd.DataFrame(),
                        table='some.table.foo',
                        write_preference='notchecked',
                        partition_date="bar",
                        partition_field="foo"
                    )
                    bq_mock.TimePartitioning.assert_called_with(type_=bq_mock.TimePartitioningType.DAY, field='foo')
                    expected_table_id = 'foo$bar'
                    bq_mock.TableReference.assert_called_with(unittest.mock.ANY, table_id=expected_table_id)

    def test_load_table_from_dataframe_partitioned_field(self):
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
                        partition_date="bar",
                        partition_field="foo"
                    )
                    client.load_table_from_dataframe(
                        data_df=pd.DataFrame(),
                        table='some.table.test',
                        write_preference='notchecked',
                        partition_date="bar",
                        partition_field="foo"
                    )
                    bq_mock.TimePartitioning.assert_called_with(type_=bq_mock.TimePartitioningType.DAY, field="foo")
                    expected_table_id = "test$bar"
                    bq_mock.TableReference.assert_called_with(unittest.mock.ANY, table_id=expected_table_id)

    @unittest.expectedFailure
    def test_run_no_query(self):
        bq_client = bq.Client(project='fake_project')
        bq_client.run_query(
        )

    @patch('google.cloud.bigquery.QueryJobConfig')
    @patch('google.cloud.bigquery.Client')
    @patch('to_data_library.data.bq.default')
    def test_run_query(self, mock_default, mock_client, mock_queryjobconfig):
        mock_default.return_value = 'first', 'second'

        mock_query = mock_client.return_value.query
        mock_query_result = mock_query.return_value
        mock_query_result.total_bytes_processed = 2000
        mocked_job_config = mock_queryjobconfig.return_value

        bq_client = bq.Client(project='fake_project')
        bq_client.run_query(
            query='SELECT * FROM {}.{} where profile_id={{{{id}}}}'.format('fake_dataset_id', 'fake_table_id'),
            params={'id': 1}
        )
        mock_queryjobconfig.assert_called_with(allow_large_results=True)
        mock_query.assert_called_with('SELECT * FROM fake_dataset_id.fake_table_id where profile_id=1',
                                      job_config=mocked_job_config)

    @unittest.expectedFailure
    def test_create_table_with_no_schema(self):
        bq_client = bq.Client('fake_project_name')
        bq_client.create_table(
            table='{}.{}.{}'.format('fake_project_id', 'fake_dataset_id', 'fake_table_id')
        )

    @patch('google.cloud.bigquery.Client')
    @patch('google.cloud.bigquery.Table')
    @patch('to_data_library.data.bq.default')
    def test_create_table_with_schema_fields(self, mock_default, mock_bq_table, mock_bigquery):
        mock_default.return_value = 'first', 'second'
        bq_client = bq.Client(project='fake_project',)
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
    @patch('to_data_library.data.bq.default')
    def test_create_table_with_schema_file_name(self, mock_default, mock_bq_table, mock_bigquery):
        mock_default.return_value = 'first', 'second'
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
