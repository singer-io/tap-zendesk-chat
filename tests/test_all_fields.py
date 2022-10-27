"""
Test that with no fields selected for a stream automatic fields are still replicated
"""
from tap_tester import runner, connections, menagerie

from base import BaseTapTest




class DynamicsAutomaticFields(BaseTapTest):
    """Test that all fields selected for a stream are replicated"""

    @staticmethod
    def name():
        return "tap_tester_dynamics_all_fields"

    def test_run(self):
        """
        When all fields are selected all fields are replicated.
        """

        expected_streams = self.expected_streams() - {"chats"}
        # temp removed chats as data isn't available for this stream, unable to create test-data using helper script

        # instantiate connection
        conn_id = connections.ensure_connection(self)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        catalog_entries = [catalog for catalog in found_catalogs
                                          if catalog.get('stream_name') in expected_streams]

        self.perform_and_verify_table_and_field_selection(conn_id, catalog_entries, expected_streams, select_all_fields=True)
        stream_all_fields = dict()
        for catalog in catalog_entries:
            stream_id, stream_name = catalog['stream_id'], catalog['stream_name']
            catalog_entry = menagerie.get_annotated_schema(conn_id, stream_id)
            fields_from_field_level_md = [md_entry['breadcrumb'][1]
                                          for md_entry in catalog_entry['metadata']
                                          if md_entry['breadcrumb'] != []]
            stream_all_fields[stream_name] = set(fields_from_field_level_md)
                
        # run initial sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_keys = stream_all_fields[stream]

                # collect actual values
                data = synced_records.get(stream)
                record_messages_keys = [set(row['data'].keys()) for row in data['messages']]


                # Verify that you get some records for each stream
                self.assertGreater(
                    record_count_by_stream.get(stream, -1), 0,
                    msg="The number of records is not over the stream max limit")

                # Verify that only the expected keys are present in all records
                for actual_keys in record_messages_keys:
                    self.assertTrue(actual_keys.issubset(expected_keys))
