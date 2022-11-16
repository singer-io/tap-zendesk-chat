"""Test that with no fields selected for a stream automatic fields are still
replicated."""
from typing import Dict

from base import ZendeskChatBaseTest
from tap_tester import connections, menagerie, runner


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
        self.assertTrue(self.start_date < return_value["start_date"])

        # Assign start date to be the default
        return_value["start_date"] = self.start_date
        return return_value

    def get_chat_type_mapping(self, conn_id: str) -> Dict:
        """performs a sync with all fields to get data on chat type mapping to
        make correct assertions based on chat type.

        returns {"chat_id":"type"}
        """

        expected_streams = self.expected_streams()
        menagerie.set_state(conn_id, {})
        found_catalogs = self.run_and_verify_check_mode(conn_id)
        catalog_entries = [catalog for catalog in found_catalogs if catalog.get("stream_name") in expected_streams]
        self.perform_and_verify_table_and_field_selection(
            conn_id, catalog_entries, expected_streams, select_all_fields=True
        )
        self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()
        data = synced_records.get("chats", {})["messages"]
        chat_type_mapping = {row["data"]["id"]: row["data"]["type"] for row in data if row["action"] == "upsert"}
        return chat_type_mapping

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

        chat_mapping = self.get_chat_type_mapping(conn_id)

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
                    for row in data["messages"]:
                        rec = row["data"]
                        actual_keys = set(rec.keys())
                        if chat_mapping[rec["id"]] == "offline_msg":
                            self.assertSetEqual(actual_keys, expected_keys_offline_msg)
                        elif chat_mapping[rec["id"]] == "chat":
                            self.assertSetEqual(actual_keys, expected_keys)
                else:
                    for actual_keys in record_messages_keys:
                        self.assertSetEqual(expected_keys, actual_keys)
