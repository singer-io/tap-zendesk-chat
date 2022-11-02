"""Test that with no fields selected for a stream automatic fields are still
replicated."""
import copy

from base import BaseTapTest
from tap_tester import connections, menagerie, runner
from tap_tester.logger import LOGGER


class TestInteruptibleSync(BaseTapTest):
    """Test that all fields selected for a stream are replicated."""

    @staticmethod
    def name():
        return "tap_tester_test_interrupted_sync"

    def test_run(self):
        """Verify the use of `currently_syncing` bookmark incase of a sync that
        was terminted/interrupted."""
        start_date = self.get_properties()["start_date"]
        expected_streams = self.expected_streams() - {"chats"}
        expected_replication_keys = self.expected_replication_keys()
        expected_replication_methods = self.expected_replication_method()

        # instantiate connection
        conn_id = connections.ensure_connection(self)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        catalog_entries = [item for item in found_catalogs if item.get("stream_name") in expected_streams]

        self.perform_and_verify_table_and_field_selection(conn_id, catalog_entries, expected_streams)

        # Run a first sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_bookmarks = menagerie.get_state(conn_id)

        completed_streams = {"account", "agents", "bans", "departments"}
        pending_streams = {"shortcuts", "triggers"}
        interrupt_stream = "goals"
        interrupted_sync_states = self.create_interrupt_sync_state(
            copy.deepcopy(first_sync_bookmarks), interrupt_stream, pending_streams, start_date
        )
        LOGGER.info(f"interrupted Bookmark after first sync: {interrupted_sync_states}")
        menagerie.set_state(conn_id, interrupted_sync_states)

        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_bookmarks = menagerie.get_state(conn_id)
        LOGGER.info(
            f"second_sync_record_count = {second_sync_record_count} \n second_sync_bookmarks = {second_sync_bookmarks}"
        )

        for stream in expected_streams:
            LOGGER.info(f"Executing for stream = {stream}")
            with self.subTest(stream=stream):
                # Expected values
                expected_replication_method = expected_replication_methods[stream]

                # Collect information for assertions from syncs 1 & 2 base on expected values
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)
                second_sync_messages = [
                    record.get("data")
                    for record in second_sync_records.get(stream, {}).get("messages", [])
                    if record.get("action") == "upsert"
                ]
                first_bookmark_value = first_sync_bookmarks.get("bookmarks", {stream: None}).get(stream)
                second_bookmark_value = second_sync_bookmarks.get("bookmarks", {stream: None}).get(stream)
                LOGGER.info(
                    f"first_bookmark_value = {first_bookmark_value} \n second_bookmark_value = {second_bookmark_value}"
                )
                if expected_replication_method == self.INCREMENTAL:
                    replication_key = next(iter(expected_replication_keys[stream]))
                    interrupted_bookmark_value = interrupted_sync_states["bookmarks"][stream]
                    if stream in completed_streams:
                        # Verify at least 1 record was replicated in the second sync
                        self.assertGreaterEqual(
                            second_sync_count,
                            1,
                            msg="Incorrect bookmarking for {}, at least one or more record should be replicated".format(
                                stream
                            ),
                        )
                    elif stream == interrupted_sync_states.get("currently_syncing", None):
                        # For interrupted stream records sync count should be less equals
                        self.assertLessEqual(
                            second_sync_count,
                            first_sync_count,
                            msg="For interrupted stream - {}, seconds sync record count should be lesser or equal to first sync".format(
                                stream
                            ),
                        )
                    elif stream in pending_streams:
                        # First sync and second sync record count match
                        self.assertGreaterEqual(
                            second_sync_count,
                            first_sync_count,
                            msg="For pending sync streams - {}, second sync record count should be more than or equal to first sync".format(
                                stream
                            ),
                        )
                elif expected_replication_method == self.FULL:
                    self.assertEqual(second_sync_count, first_sync_count)
                else:
                    raise NotImplementedError(
                        "INVALID EXPECTATIONS\t\tSTREAM: {} REPLICATION_METHOD: {}".format(
                            stream, expected_replication_method
                        )
                    )
