import os
import datetime
import dateutil.parser
import pytz

from tap_tester import runner, menagerie, connections

from base import BaseTapTest


STREAMS_WITH_BOOKMARKS = ['agents', 'chats']

class BookmarksTest(BaseTapTest):

    expected_record_count = {
        'agents': 3,
        'chats': 223,
        'bans': 22,
        'account': 1,
        'shortcuts': 4,
        'triggers': 12,
        'departments': 1,
        'goals': 2,
    }

    @staticmethod
    def name():
        return "tap_tester_zendesk_chat_bookmarks"

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        return_value = {
            'start_date': '2017-08-15T00:00:00Z',
            'agents_page_limit': 1,
        }
        if original:
            return return_value

        return_value["start_date"] = self.start_date

        return return_value


    def test_run(self):
        expected_streams =  self.expected_streams()

        # Testing against ads insights objects
        self.start_date = self.get_properties()['start_date']

        """A Parametrized Bookmarks Test"""
        expected_replication_keys = self.expected_replication_keys()
        expected_replication_methods = self.expected_replication_method()

        ##########################################################################
        ### First Sync
        ##########################################################################

        conn_id = connections.ensure_connection(self, original_properties=False)

        # Run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Select only the expected streams tables
        catalog_entries = [ce for ce in found_catalogs if ce['tap_stream_id'] in expected_streams]
        self.perform_and_verify_table_and_field_selection(conn_id, catalog_entries, expected_streams, select_all_fields=True)

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)


        ##########################################################################
        ### Second Sync
        ##########################################################################

        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_bookmarks = menagerie.get_state(conn_id)

        ##########################################################################
        ### Test By Stream
        ##########################################################################

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_replication_method = expected_replication_methods[stream]

                # collect information for assertions from syncs 1 & 2 base on expected values
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)
                first_sync_messages = [record.get('data') for record in
                                       first_sync_records.get(stream).get('messages')
                                       if record.get('action') == 'upsert']
                second_sync_messages = [record.get('data') for record in
                                        second_sync_records.get(stream).get('messages')
                                        if record.get('action') == 'upsert']
                first_bookmark_key_value = first_sync_bookmarks.get('bookmarks', {}).get(stream)
                second_bookmark_key_value = second_sync_bookmarks.get('bookmarks', {}).get(stream)

                # Assert we synced the expected number of records. Ensures pagination happens
                self.assertEqual(first_sync_count, self.expected_record_count[stream])


                if expected_replication_method == self.INCREMENTAL: # chats is the only incremental stream

                    # collect information specific to incremental streams from syncs 1 & 2
                    replication_key = next(iter(expected_replication_keys[stream]))

                    # Verify the first sync sets a bookmark of the expected form
                    self.assertIsNotNone(first_bookmark_key_value)
                    self.assertIsNotNone(first_bookmark_key_value.get('chat.end_timestamp'))
                    self.assertIsNotNone(first_bookmark_key_value.get('offline_msg.timestamp'))

                    # Verify the second sync sets a bookmark of the expected form
                    self.assertIsNotNone(second_bookmark_key_value)
                    self.assertIsNotNone(second_bookmark_key_value.get('chat.end_timestamp'))
                    self.assertIsNotNone(second_bookmark_key_value.get('offline_msg.timestamp'))

                    # Verify the second sync bookmark is Equal to the first sync bookmark
                    self.assertEqual(second_bookmark_key_value, first_bookmark_key_value) # assumes no changes to data during test

                    for record in second_sync_messages:

                        if record.get('type') == 'chat':
                            # Verify the second sync records respect the previous (simulated) bookmark value
                            replication_key_value = record.get('end_timestamp')

                            # Verify the second sync bookmark value is the max replication key value for a given stream
                            self.assertLessEqual(
                                replication_key_value,
                                second_bookmark_key_value.get('chat.end_timestamp'),
                                msg="Second sync bookmark was set incorrectly, a record with a greater replication-key value was synced.")

                        elif record.get('type') == 'offline_msg':
                            # Verify the second sync records respect the previous (simulated) bookmark value
                            replication_key_value = record.get('timestamp')

                            # Verify the second sync bookmark value is the max replication key value for a given stream
                            self.assertLessEqual(
                                replication_key_value,
                                second_bookmark_key_value.get('offline_msg.timestamp'),
                                msg="Second sync bookmark was set incorrectly, a record with a greater replication-key value was synced.")

                        else:
                            assert(False)

                    for record in first_sync_messages:
                        if record.get('type') == 'chat':
                            # Verify the first sync records respect the previous (simulated) bookmark value
                            replication_key_value = record.get('end_timestamp')

                            # Verify the second sync bookmark value is the max replication key value for a given stream
                            self.assertLessEqual(
                                replication_key_value,
                                first_bookmark_key_value.get('chat.end_timestamp'),
                                msg="First sync bookmark was set incorrectly, a record with a greater replication-key value was synced.")

                        elif record.get('type') == 'offline_msg':
                            # Verify the first sync records respect the previous (simulated) bookmark value
                            replication_key_value = record.get('timestamp')

                            # Verify the first sync bookmark value is the max replication key value for a given stream
                            self.assertLessEqual(
                                replication_key_value,
                                first_bookmark_key_value.get('offline_msg.timestamp'),
                                msg="First sync bookmark was set incorrectly, a record with a greater replication-key value was synced.")

                        else:
                            assert(False)

                    # Verify the number of records in the 2nd sync is less then the first
                    self.assertLess(second_sync_count, first_sync_count)


                elif expected_replication_method == self.FULL:
                    # Verify the number of records in the second sync is the same as the first
                    self.assertEqual(second_sync_count, first_sync_count)

                    if stream in ('agents', 'bans'):
                        self.assertEqual(first_bookmark_key_value, second_bookmark_key_value, {'offset': {'id': None}})
                    else:
                        # Verify the syncs do not set a bookmark for full table streams
                        self.assertIsNone(first_bookmark_key_value)
                        self.assertIsNone(second_bookmark_key_value)

                else:
                    raise NotImplementedError(
                        "INVALID EXPECTATIONS\t\tSTREAM: {} REPLICATION_METHOD: {}".format(stream, expected_replication_method))

                # Verify at least 1 record was replicated in the second sync
                self.assertGreater(second_sync_count, 0, msg="We are not fully testing bookmarking for {}".format(stream))
