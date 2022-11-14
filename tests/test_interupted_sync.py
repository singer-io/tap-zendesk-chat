"""Test that with no fields selected for a stream automatic fields are still
replicated."""
import copy

from base import ZendeskChatBaseTest
from tap_tester import connections, menagerie, runner
from tap_tester.logger import LOGGER


class TestZendeskChatDiscoveryInteruptibleSync(ZendeskChatBaseTest):
    """Test tap's ability to recover from an interrupted sync."""

    @staticmethod
    def name():
        return "tap_tester_zendesk_chat_interrupted_sync"

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        return_value = {"start_date": "2022-10-10T00:00:00Z", "chat_search_interval_days": 1}
        if original:
            return return_value

        return_value["start_date"] = self.start_date

        return return_value

    def test_run(self):
        """Testing that if a sync job is interrupted and state is saved with
        `currently_syncing`(stream) the next sync job kicks off and the tap
        picks back up on that `currently_syncing` stream.

        - Verify behavior is consistent when an added stream is selected between initial and resuming sync
        """

        start_date = self.get_properties()["start_date"]
        expected_streams = self.expected_streams()

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

        completed_streams = {"account", "agents", "bans", "chats", "departments"}
        pending_streams = {"shortcuts", "triggers"}
        interrupt_stream = "goals"
        interrupted_sync_states = self.create_interrupt_sync_state(
            copy.deepcopy(first_sync_bookmarks), interrupt_stream, pending_streams, start_date
        )
        menagerie.set_state(conn_id, interrupted_sync_states)
        second_sync_record_count = self.run_and_verify_sync(conn_id)

        for stream in expected_streams:
            with self.subTest(stream=stream):
                expected_replication_method = expected_replication_methods[stream]
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)

                if expected_replication_method == self.INCREMENTAL:

                    if stream in completed_streams:
                        # Verify at least 1 record was replicated in the second sync
                        self.assertGreaterEqual(
                            second_sync_count,
                            1,
                            msg=f"Incorrect bookmarking for {stream}, at least one or more record should be replicated",
                        )

                    elif stream == interrupted_sync_states.get("currently_syncing", None):
                        # For interrupted stream records sync count should be less equals
                        self.assertLessEqual(
                            second_sync_count,
                            first_sync_count,
                            msg=f"For interrupted stream - {stream}, seconds sync record count should be lesser or equal to first sync",
                        )
                    elif stream in pending_streams:
                        # First sync and second sync record count match
                        self.assertGreaterEqual(
                            second_sync_count,
                            first_sync_count,
                            msg=f"For pending sync streams - {stream}, second sync record count should be more than or equal to first sync",
                        )

                elif expected_replication_method == self.FULL:
                    self.assertEqual(second_sync_count, first_sync_count)
                else:
                    raise NotImplementedError(
                        f"INVALID EXPECTATIONS: STREAM: {stream} REPLICATION_METHOD: {expected_replication_method}"
                    )
