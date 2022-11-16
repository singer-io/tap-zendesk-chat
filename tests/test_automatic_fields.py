"""Test that with no fields selected for a stream automatic fields are still
replicated."""
from base import ZendeskChatBaseTest
from tap_tester import connections, runner
from tap_tester.logger import LOGGER


class TestZendeskChatAutomaticFields(ZendeskChatBaseTest):
    """Test that with no fields selected for a stream automatic fields are
    still replicated."""

    @staticmethod
    def name():
        return "tap_tester_zendesk_chat_automatic_fields"

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""

        return_value = {
            "start_date": "2017-01-15T00:00:00Z",
            "chat_search_interval_days": 500,
        }

        if original:
            return return_value

        # Start Date test needs the new connections start date to be prior to the default
        assert self.start_date < return_value["start_date"]

        # Assign start date to be the default
        return_value["start_date"] = self.start_date
        return return_value

    def test_run(self):
        """
        - Verify we can deselect all fields except when inclusion=automatic, which is handled by base.py methods
        - Verify that only the automatic fields are sent to the target.
        - Verify that all replicated records have unique primary key values.
        """

        expected_streams = self.expected_streams()

        conn_id = connections.ensure_connection(self)
        found_catalogs = self.run_and_verify_check_mode(conn_id)
        catalog_entries = [catalog for catalog in found_catalogs if catalog.get("stream_name") in expected_streams]
        self.perform_and_verify_table_and_field_selection(
            conn_id, catalog_entries, expected_streams, select_all_fields=False
        )

        # run initial sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):

                expected_keys = self.expected_automatic_fields().get(stream)

                data = synced_records.get(stream, {})
                record_messages_keys = [set(row["data"].keys()) for row in data["messages"]]

                self.assertGreater(
                    record_count_by_stream.get(stream, -1),
                    0,
                    msg="The number of records is not over the stream max limit",
                )
                if stream == "chats":
                    # chats stream has two types of records "offline_msgs" and "chat" both of them have different replication keys
                    # the key "end_timestamp" is not available for "offline_msgs" 
                    # hence we need to verify the record has both or atleaset one key
                    expected_keys_offline_msg = self.expected_automatic_fields().get(stream) - {"end_timestamp"}
                    for actual_keys in record_messages_keys:
                        if actual_keys == expected_keys:
                            pass
                        elif actual_keys == expected_keys_offline_msg:
                            pass
                        else:
                            self.fail(f"Record of type: chat does not have the following automatic fields {expected_keys_offline_msg-actual_keys}")
                else:
                    for actual_keys in record_messages_keys:
                        self.assertSetEqual(expected_keys, actual_keys)
