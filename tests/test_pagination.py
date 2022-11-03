"""Test that with no fields selected for a stream automatic fields are still
replicated."""
import copy
from math import ceil

from base import BaseTapTest
from tap_tester import connections, menagerie, runner
from tap_tester.logger import LOGGER


class TestPagination(BaseTapTest):
    """Test that all fields selected for a stream are replicated."""

    AGENTS_PAGE_SIZE = 1
    BANS_PAGE_SIZE = 100

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        return_value = {
            "start_date": "2021-10-10T00:00:00Z",
            "agents_page_limit": self.AGENTS_PAGE_SIZE,
            # "bans_page_limit":self.BANS_PAGE_SIZE
        }
        if original:
            return return_value

        return_value["start_date"] = self.start_date

        return return_value

    @staticmethod
    def name():
        return "tap_tester_test_pagination"

    def test_run(self):
        """Verify the use of `currently_syncing` bookmark in case of a sync
        that was terminted/interrupted."""
        page_size = int(self.get_properties().get("agents_page_limit", 10))
        expected_streams = {"bans", "agents"}
        # only "bans" and "agents" stream support pagination
        # instantiate connection
        conn_id = connections.ensure_connection(self)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        catalog_entries = [item for item in found_catalogs if item.get("stream_name") in expected_streams]

        self.perform_and_verify_table_and_field_selection(conn_id, catalog_entries, expected_streams)

        # Run a first sync job using orchestrator
        synced_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):
                page_size = self.BANS_PAGE_SIZE if stream == "bans" else self.AGENTS_PAGE_SIZE
                # expected values
                expected_primary_keys = self.expected_primary_keys()
                # collect information for assertions from syncs 1 & 2 base on expected values
                primary_keys_list = [
                    tuple(message.get("data").get(expected_pk) for expected_pk in expected_primary_keys[stream])
                    for message in synced_records.get(stream).get("messages")
                    if message.get("action") == "upsert"
                ]
                LOGGER.info("stream: %s pk_list %s", stream, primary_keys_list)
                # verify records are more than page size so multiple page is working
                # Chunk the replicated records (just primary keys) into expected pages
                pages = []
                page_count = ceil(len(primary_keys_list) / page_size)
                for page_index in range(page_count):
                    page_start = page_index * page_size
                    page_end = (page_index + 1) * page_size
                    pages.append(set(primary_keys_list[page_start:page_end]))

                LOGGER.info("items: %s page_count %s", len(primary_keys_list), page_count)

                # Verify by primary keys that data is unique for each page
                for current_index, current_page in enumerate(pages):
                    with self.subTest(current_page_primary_keys=current_page):
                        for other_index, other_page in enumerate(pages):
                            if current_index == other_index:
                                continue  # don't compare the page to itself
                            self.assertTrue(
                                current_page.isdisjoint(other_page), msg=f"other_page_primary_keys={other_page}"
                            )