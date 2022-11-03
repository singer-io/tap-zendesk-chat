"""Test that with no fields selected for a stream automatic fields are still
replicated."""
from base import BaseTapTest
from tap_tester import connections, runner


class DynamicsAutomaticFields(BaseTapTest):
    """Test that with no fields selected for a stream automatic fields are
    still replicated."""

    @staticmethod
    def name():
        return "tap_tester_dynamics_automatic_fields"

    def test_run(self):
        """Verify that for each stream you can get multiple pages of data when
        no fields are selected and only the automatic fields are replicated.

        PREREQUISITE For EACH stream add enough data that you surpass
        the limit of a single fetch of data.  For instance if you have a
        limit of 250 records ensure that 251 (or more) records have been
        posted for that stream.
        """

        expected_streams = self.expected_streams()
        # removed chats as data isn't available for this stream, unable to create test-data using helper script

        # instantiate connection
        conn_id = connections.ensure_connection(self)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        catalog_entries = [catalog for catalog in found_catalogs if catalog.get("stream_name") in expected_streams]

        self.perform_and_verify_table_and_field_selection(
            conn_id, catalog_entries, expected_streams, select_all_fields=False
        )

        # run initial sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_keys = self.expected_automatic_fields().get(stream)

                # collect actual values
                data = synced_records.get(stream)
                record_messages_keys = [set(row["data"].keys()) for row in data["messages"]]

                # Verify that you get some records for each stream
                self.assertGreater(
                    record_count_by_stream.get(stream, -1),
                    0,
                    msg="The number of records is not over the stream max limit",
                )

                if stream == "chats":
                    expected_keys.remove("end_timestamp")
                # Verify that only the automatic fields are sent to the target
                for actual_keys in record_messages_keys:
                    self.assertSetEqual(expected_keys, actual_keys)