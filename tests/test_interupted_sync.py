"""
Test that with no fields selected for a stream automatic fields are still replicated
"""
from tap_tester import runner, connections, menagerie

from base import BaseTapTest
from tap_tester.logger import LOGGER




class TestInteruptibleSync(BaseTapTest):
    """Test that all fields selected for a stream are replicated"""

    @staticmethod
    def name():
        return "tap_tester_test_interupted_sync"

    def test_run(self):
        """
        Verify the use of `currently_syncing` bookmark incase of a sync that was terminted/interupted
        """

        expected_streams = self.expected_streams()
        expected_replication_keys = self.expected_replication_keys()
        expected_replication_methods = self.expected_replication_method()
        LOGGER.info(f"expected_replication_keys = %s \n expected_replication_methods = %s",expected_replication_keys,expected_replication_methods)

        # temp removed chats as data isn't available for this stream, unable to create test-data using helper script

        # instantiate connection
        conn_id = connections.ensure_connection(self)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        catalog_entries = [item for item in found_catalogs if item.get('stream_name') in expected_streams]

        self.perform_and_verify_table_and_field_selection(conn_id, catalog_entries, expected_streams)
                
        # Run a first sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_bookmarks = menagerie.get_state(conn_id)
        
        LOGGER.info("first_sync_record_count = %s",first_sync_record_count)
        LOGGER.info("Current Bookmark after first sync: {}".format(first_sync_bookmarks))
