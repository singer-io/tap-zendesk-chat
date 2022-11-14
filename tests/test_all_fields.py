"""Test that with no fields selected for a stream automatic fields are still
replicated."""
from base import ZendeskChatBaseTest
from tap_tester import connections, menagerie, runner


class TestZendeskChatAllFields(ZendeskChatBaseTest):
    """Test that all fields selected for a stream are replicated."""

    @staticmethod
    def name():
        return "tap_tester_zendesk_chat_all_fields"

    KNOWN_MISSING_FIELDS = {
        "agents": {
            "scope",
        },
        "account": {
            "billing",
        },
        "shortcuts": {
            "departments",
            "agents",
        },
    }

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
        - Verify no unexpected streams were replicated
        - Verify that more than just the automatic fields are replicated for each stream.
        - Verify all fields for each stream are replicated
        """
        expected_streams = self.expected_streams()
        conn_id = connections.ensure_connection(self)
        found_catalogs = self.run_and_verify_check_mode(conn_id)
        catalog_entries = [catalog for catalog in found_catalogs if catalog.get("stream_name") in expected_streams]
        self.perform_and_verify_table_and_field_selection(
            conn_id, catalog_entries, expected_streams, select_all_fields=True
        )
        stream_all_fields = dict()

        for catalog in catalog_entries:
            stream_id, stream_name = catalog["stream_id"], catalog["stream_name"]
            catalog_entry = menagerie.get_annotated_schema(conn_id, stream_id)
            fields_from_field_level_md = [
                md_entry["breadcrumb"][1] for md_entry in catalog_entry["metadata"] if md_entry["breadcrumb"] != []
            ]
            stream_all_fields[stream_name] = set(fields_from_field_level_md)

        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):

                expected_all_keys = stream_all_fields[stream]
                expected_automatic_keys = self.expected_automatic_fields().get(stream)
                data = synced_records.get(stream)
                actual_all_keys = set()

                for message in data["messages"]:
                    if message["action"] == "upsert":
                        actual_all_keys.update(message["data"].keys())

                self.assertTrue(
                    expected_automatic_keys.issubset(expected_all_keys),
                    msg=f'{expected_automatic_keys-expected_all_keys} is not in "expected_all_keys"',
                )

                self.assertGreater(len(expected_all_keys), len(expected_automatic_keys))

                expected_all_keys = expected_all_keys - self.KNOWN_MISSING_FIELDS.get(stream, set())

                self.assertGreater(
                    record_count_by_stream.get(stream, -1),
                    0,
                    msg="The number of records is not over the stream max limit",
                )
                self.assertSetEqual(expected_all_keys, actual_all_keys)
