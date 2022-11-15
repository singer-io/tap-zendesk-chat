"""Test that with no fields selected for a stream automatic fields are still
replicated."""
import copy

from base import ZendeskChatBaseTest
from tap_tester import connections, menagerie, runner
from tap_tester.logger import LOGGER
from singer.utils import strptime_to_utc


class TestZendeskChatDiscoveryInteruptibleSync(ZendeskChatBaseTest):
    """Test tap's ability to recover from an interrupted sync."""

    @staticmethod
    def name():
        return "tap_tester_zendesk_chat_interrupted_sync"

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        return_value = {"start_date": "2017-01-10T00:00:00Z", "chat_search_interval_days": 300}
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
        # skipping following stremas {"goals","shortcuts", "triggers"}
        expected_streams = {"account", "agents","bans","chats","departments"}

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
        first_sync_records = runner.get_records_from_target_output()

        first_sync_bookmarks = menagerie.get_state(conn_id)

        completed_streams = {"account", "agents", "bans"}
        interrupt_stream = "chats"
        pending_streams = {"departments"}
        interrupted_sync_states = copy.deepcopy(first_sync_bookmarks)
        bookmark_state = interrupted_sync_states["bookmarks"]
        # set the interrupt stream as currently syncing
        interrupted_sync_states["currently_syncing"] = interrupt_stream

        # remove bookmark for completed streams to set them as pending
        # setting value to start date wont be needed as all other streams are full_table
        for stream in pending_streams:
            bookmark_state.pop(stream,None)

        # update state for chats stream and set the bookmark to a date earlier
        chats_bookmark = bookmark_state.get("chats",{})
        chats_bookmark.pop("offset",None)
        chats_rec,offline_msgs_rec = [],[]
        for record in first_sync_records.get("chats").get("messages"):
            if record.get("action") == "upsert":
                rec = record.get("data")
                if rec["type"] == "offline_msg":
                    offline_msgs_rec.append(rec)
                else:
                    chats_rec.append(rec)

        # set a deffered bookmark value for both the bookmarks of chat stream 
        if len(chats_rec) > 1:
            chats_bookmark["chat.end_timestamp"] = chats_rec[-1]["end_timestamp"]
        if len(offline_msgs_rec) > 1:
            chats_bookmark["offline_msg.timestamp"] = offline_msgs_rec[-1]["timestamp"]

        bookmark_state["chats"] = chats_bookmark
        interrupted_sync_states["bookmarks"] = bookmark_state
        menagerie.set_state(conn_id, interrupted_sync_states)

        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):
                expected_replication_method = expected_replication_methods[stream]
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)

                # gather results
                full_records = [message['data'] for message in first_sync_records[stream]['messages']]
                interrupted_records = [message['data'] for message in second_sync_records[stream]['messages']]

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

                        # Verify the interrupted sync replicates the expected record set
                        # All interrupted recs are in full recs
                        for record in interrupted_records:
                                self.assertIn(
                                    record,
                                    full_records,
                                    msg='incremental table record in interrupted sync not found in full sync'
                                )

                        # Verify resuming sync only replicates records with replication key values greater or equal to
                        # the interrupted_state for streams that were replicated during the interrupted sync.    
                        if stream == "chats":
                           
                            interrupted_bmk_chat_msg = strptime_to_utc(bookmark_state["chats"]["offline_msg.timestamp"])
                            interrupted_bmk_chat = strptime_to_utc(bookmark_state["chats"]["chat.end_timestamp"])

                            for record in interrupted_records:
                                if record["type"] == "offline_msg":
                                    rec_time = strptime_to_utc(record.get("timestamp"))
                                    self.assertGreaterEqual(rec_time, interrupted_bmk_chat_msg)
                                else:
                                    rec_time = strptime_to_utc(record.get("end_timestamp"))
                                    self.assertGreaterEqual(rec_time, interrupted_bmk_chat)

                            # Record count for all streams of interrupted sync match expectations
                            full_records_after_interrupted_bookmark = 0
                            
                            for record in full_records:
                                if record["type"] == "offline_msg":
                                    rec_time = strptime_to_utc(record.get("timestamp"))
                                    if rec_time >= interrupted_bmk_chat_msg:
                                        full_records_after_interrupted_bookmark += 1
                                else:
                                    rec_time = strptime_to_utc(record.get("end_timestamp"))
                                    if rec_time >= interrupted_bmk_chat:
                                        full_records_after_interrupted_bookmark += 1
                            
                            self.assertEqual(
                                full_records_after_interrupted_bookmark,
                                len(interrupted_records),
                                msg=f"Expected {full_records_after_interrupted_bookmark} records in each sync"
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
