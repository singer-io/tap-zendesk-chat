"""Test that with no fields selected for a stream automatic fields are still
replicated."""
from base import BaseTapTest
from tap_tester import connections, runner
from tap_tester.logger import LOGGER


class TestZendeskChatAutomaticFields(BaseTapTest):
    """Test that with no fields selected for a stream automatic fields are still replicated"""

    @staticmethod
    def name():
        return "tap_tester_zendesk_chat_automatic_fields"

    def test_run(self):
        """
        - Verify we can deselect all fields except when inclusion=automatic, which is handled by base.py methods
        - Verify that only the automatic fields are sent to the target.
        - Verify that all replicated records have unique primary key values.
        """

        expected_streams = self.expected_streams() - {"chats"}
        # excluding chats stream because it contains two types of records "offline_msg" and "chat",
        # "end_timestamp" is a replication key which is not available for "offline_msg" type of records 
        # excluding this stream as an execption for this test.

        conn_id = connections.ensure_connection(self)
        found_catalogs = self.run_and_verify_check_mode(conn_id)
        catalog_entries = [catalog for catalog in found_catalogs if catalog.get("stream_name") in expected_streams]
        self.perform_and_verify_table_and_field_selection(conn_id, catalog_entries, expected_streams, select_all_fields=False)

        # run initial sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):

                expected_keys = self.expected_automatic_fields().get(stream)

                data = synced_records.get(stream,{})
                record_messages_keys = [set(row["data"].keys()) for row in data["messages"]]

                self.assertGreater(record_count_by_stream.get(stream, -1),0,
                    msg="The number of records is not over the stream max limit",
                )
                # if stream == "chats":
                #     expected_keys_offline_msgs = self.expected_automatic_fields().get(stream) - {"end_timestamp"}
                #     for row in data["messages"]:
                #         record = row["data"]
                #         actual_keys = set(record.keys())
                #         if record.get("type","") == "chat":
                #             self.assertSetEqual(expected_keys,actual_keys)
                #         else:
                #             LOGGER.info("%s",record)
                #             self.assertSetEqual(expected_keys_offline_msgs, actual_keys)
                # else:
                for actual_keys in record_messages_keys:
                    self.assertSetEqual(expected_keys, actual_keys)


    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""

        return_value = {
            "start_date": "2017-08-15T00:00:00Z",
            "chat_search_interval_days": 500,
            }

        if original:
            return return_value

        # Start Date test needs the new connections start date to be prior to the default
        assert self.start_date < return_value["start_date"]

        # Assign start date to be the default
        return_value["start_date"] = self.start_date
        return return_value