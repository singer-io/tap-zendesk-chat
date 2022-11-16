"""Setup expectations for test sub classes Run discovery for as a prerequisite
for most tests."""
import copy
import json
import os
import unittest
from datetime import datetime as dt
from datetime import timezone as tz
from typing import Any, Dict, Set

from tap_tester import connections, menagerie, runner


class ZendeskChatBaseTest(unittest.TestCase):
    REPLICATION_KEYS = "valid-replication-keys"
    PRIMARY_KEYS = "table-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    INCREMENTAL = "INCREMENTAL"
    FULL = "FULL_TABLE"
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_date = self.get_properties().get("start_date")
        self.maxDiff = None

    @staticmethod
    def tap_name():
        """The name of the tap."""
        return "tap-zendesk-chat"

    @staticmethod
    def get_type():
        """the expected url route ending."""
        return "platform.zendesk-chat"

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""

        return_value = {"start_date": dt.strftime(dt.today(), self.START_DATE_FORMAT)}

        if original:
            return return_value

        # Start Date test needs the new connections start date to be prior to the default
        self.assertTrue(self.start_date < return_value["start_date"])

        # Assign start date to be the default
        return_value["start_date"] = self.start_date
        return return_value

    @staticmethod
    def get_credentials():
        """Authentication information for the test account."""
        return {"access_token": os.getenv("TAP_ZENDESK_CHAT_ACCESS_TOKEN")}

    def expected_metadata(self):
        """The expected streams and metadata about the streams."""

        default = {
            self.PRIMARY_KEYS: {"id"},
            self.REPLICATION_METHOD: self.FULL
        }

        shortcuts_rep_key = {
            self.PRIMARY_KEYS: {"name"},
            self.REPLICATION_METHOD: self.FULL
        }

        account_rep_key = {
            self.PRIMARY_KEYS: {"account_key"},
            self.REPLICATION_METHOD: self.FULL
        }

        chats_rep_key = {
            self.PRIMARY_KEYS: {"id"},
            self.REPLICATION_KEYS: {"timestamp", "end_timestamp"},
            self.REPLICATION_METHOD: self.INCREMENTAL,
        }

        return {
            "agents": default,
            "chats": chats_rep_key,
            "shortcuts": shortcuts_rep_key,
            "triggers": default,
            "bans": default,
            "departments": default,
            "goals": default,
            "account": account_rep_key,
        }

    def expected_streams(self) -> Set:
        """A set of expected stream names."""
        return set(self.expected_metadata().keys())

    def expected_primary_keys(self) -> Dict:
        """return a dictionary with key of table name and value as a set of
        primary key fields."""
        return {
            table: properties.get(self.PRIMARY_KEYS, set()) for table, properties in self.expected_metadata().items()
        }

    def expected_replication_keys(self) -> Dict:
        """return a dictionary with key of table name and value as a set of
        replication key fields."""
        return {
            table: properties.get(self.REPLICATION_KEYS, set())
            for table, properties in self.expected_metadata().items()
        }

    def expected_automatic_fields(self) -> Dict:
        return {
            table: self.expected_primary_keys().get(table) | self.expected_replication_keys().get(table)
            for table in self.expected_metadata()
        }

    def expected_replication_method(self) -> Dict:
        """return a dictionary with key of table name and value of replication
        method."""
        return {
            table: properties.get(self.REPLICATION_METHOD, None)
            for table, properties in self.expected_metadata().items()
        }

    def setUp(self):
        """Verify that you have set the prerequisites to run the tap (creds,
        etc.)"""
        env_keys = {"TAP_ZENDESK_CHAT_ACCESS_TOKEN"}
        missing_envs = [x for x in env_keys if os.getenv(x) is None]
        if missing_envs:
            raise Exception(f"Set environment variables: {missing_envs}")

    #########################
    #   Helper Methods      #
    #########################

    def run_sync(self, conn_id: int):
        """Run a sync job and make sure it exited properly.

        Return a dictionary with keys of streams synced and values of
        records synced for each stream
        """
        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify actual rows were synced
        sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys()
        )
        return sync_record_count

    @staticmethod
    def local_to_utc(date: dt):
        """Convert a datetime with timezone information to utc."""
        utc = dt(date.year, date.month, date.day, date.hour, date.minute, date.second, date.microsecond, tz.utc)

        if date.tzinfo and hasattr(date.tzinfo, "_offset"):
            utc += date.tzinfo._offset

        return utc

    def max_bookmarks_by_stream(self, sync_records: Any):
        """Return the maximum value for the replication key for the events
        stream which is the bookmark expected value for updated records.

        Comparisons are based on the class of the bookmark value. Dates
        will be string compared which works for ISO date-time strings.
        """
        max_bookmarks = {}
        chats_offline = []
        chats = []
        for stream, batch in sync_records.items():
            upsert_messages = [m for m in batch.get("messages") if m["action"] == "upsert"]
            if stream == "chats":
                for msg in upsert_messages:
                    if msg["data"]["type"] == "chat":
                        chats.append(msg)
                    elif msg["data"]["type"] == "offline_msg":
                        chats_offline.append(msg)
                    else:
                        raise RuntimeError("Got unexpected chat type: " + msg["data"]["type"])
                chats_bookmark_key = "end_timestamp"
                chats_offline_bookmark_key = "timestamp"
                bk_values_chats = [message["data"].get(chats_bookmark_key) for message in chats]
                bk_values_chats_offline = [message["data"].get(chats_offline_bookmark_key) for message in chats_offline]
                max_bookmarks["chats.chat"] = {chats_bookmark_key: max(bk_values_chats, default=None)}
                max_bookmarks["chats.offline_msg"] = {
                    chats_offline_bookmark_key: max(bk_values_chats_offline, default=None)
                }
            else:

                stream_bookmark_key = self.expected_replication_keys().get(stream) or set()
                with self.subTest(stream=stream):
                    assert (
                        not stream_bookmark_key or len(stream_bookmark_key) == 1
                    )  # There shouldn't be a compound replication key
                if not stream_bookmark_key:
                    continue
                stream_bookmark_key = stream_bookmark_key.pop()

                bk_values = [message["data"].get(stream_bookmark_key) for message in upsert_messages]
                max_bookmarks[stream] = {stream_bookmark_key: None}
                for bk_value in bk_values:
                    if bk_value is None:
                        continue

                    if max_bookmarks[stream][stream_bookmark_key] is None:
                        max_bookmarks[stream][stream_bookmark_key] = bk_value

                    if bk_value > max_bookmarks[stream][stream_bookmark_key]:
                        max_bookmarks[stream][stream_bookmark_key] = bk_value
        return max_bookmarks

    def min_bookmarks_by_stream(self, sync_records: Any):
        """Return the minimum value for the replication key for each stream."""
        min_bookmarks = {}
        chats = []
        chats_offline = []
        for stream, batch in sync_records.items():
            upsert_messages = [m for m in batch.get("messages") if m["action"] == "upsert"]
            if stream == "chats":
                for msg in upsert_messages:
                    if msg["data"]["type"] == "chat":
                        chats.append(msg)
                    elif msg["data"]["type"] == "offline_msg":
                        chats_offline.append(msg)
                    else:
                        raise RuntimeError("Got unexpected chat type: " + msg["data"]["type"])
                chats_bookmark_key = "end_timestamp"
                chats_offline_bookmark_key = "timestamp"
                bk_values_chats = [message["data"].get(chats_bookmark_key) for message in chats]
                bk_values_chats_offline = [message["data"].get(chats_offline_bookmark_key) for message in chats_offline]
                min_bookmarks["chats.chat"] = {chats_bookmark_key: min(bk_values_chats, default=None)}
                min_bookmarks["chats.offline_msg"] = {
                    chats_offline_bookmark_key: min(bk_values_chats_offline, default=None)
                }
            else:
                stream_bookmark_key = self.expected_replication_keys().get(stream) or set()
                with self.subTest(stream=stream):
                    assert (
                        not stream_bookmark_key or len(stream_bookmark_key) == 1
                    )  # There shouldn't be a compound replication key
                if not stream_bookmark_key:
                    continue
                stream_bookmark_key = stream_bookmark_key.pop()

                bk_values = [message["data"].get(stream_bookmark_key) for message in upsert_messages]
                min_bookmarks[stream] = {stream_bookmark_key: None}
                for bk_value in bk_values:
                    if bk_value is None:
                        continue

                    if min_bookmarks[stream][stream_bookmark_key] is None:
                        min_bookmarks[stream][stream_bookmark_key] = bk_value

                    if bk_value < min_bookmarks[stream][stream_bookmark_key]:
                        min_bookmarks[stream][stream_bookmark_key] = bk_value
        return min_bookmarks

    def select_all_streams_and_fields(
        self, conn_id: Any, catalogs: Any, select_all_fields: bool = True, exclude_streams=None
    ):
        """Select all streams and all fields within streams."""

        for catalog in catalogs:
            if exclude_streams and catalog.get("stream_name") in exclude_streams:
                continue
            schema = menagerie.get_annotated_schema(conn_id, catalog["stream_id"])
            non_selected_properties = []
            if not select_all_fields:
                # get a list of all properties so that none are selected
                non_selected_properties = schema.get("annotated-schema", {}).get("properties", {})
                # remove properties that are automatic
                for prop in self.expected_automatic_fields().get(catalog["stream_name"], []):
                    if prop in non_selected_properties:
                        del non_selected_properties[prop]
                non_selected_properties = non_selected_properties.keys()
            additional_md = []

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema, additional_md=additional_md, non_selected_fields=non_selected_properties
            )

    def create_connection(self, original_properties: bool = True, original_credentials: bool = True):
        """Create a new connection with the test name."""
        # Create the connection
        conn_id = connections.ensure_connection(self, original_properties, original_credentials)

        # Run a check job using orchestrator (discovery)
        check_job_name = runner.run_check_mode(self, conn_id)

        # Assert that the check job succeeded
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)
        return conn_id

    @staticmethod
    def get_selected_fields_from_metadata(metadata):
        selected_fields = set()
        for field in metadata:
            is_field_metadata = len(field["breadcrumb"]) > 1
            if is_field_metadata:
                inclusion_automatic_or_selected = (
                    field["metadata"]["inclusion"] == "automatic" or field["metadata"]["selected"] is True
                )
                if inclusion_automatic_or_selected:
                    selected_fields.add(field["breadcrumb"][1])
        return selected_fields

    def run_and_verify_check_mode(self, conn_id: Any):
        """Run the tap in check mode and verify it succeeds. This should be ran
        prior to field selection and initial sync.

        Return the connection id and found catalogs from menagerie.
        """
        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)

        self.assertGreater(len(found_catalogs), 0, msg=f"unable to locate schemas for connection {conn_id}")
        found_catalog_names = set(map(lambda c: c["tap_stream_id"], found_catalogs))
        diff = self.expected_streams().symmetric_difference(found_catalog_names)
        self.assertEqual(len(diff), 0, msg=f"discovered schemas do not match: {diff}")
        return found_catalogs

    def run_and_verify_sync(self, conn_id, clear_state: bool = False):
        """Clear the connections state in menagerie and Run a Sync. Verify the
        exit code following the sync.

        Return the connection id and record count by stream
        """
        if clear_state:
            # clear state
            menagerie.set_state(conn_id, {})

        # run sync
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # read target output
        record_count_by_stream = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys()
        )

        return record_count_by_stream

    def perform_and_verify_table_and_field_selection(
        self, conn_id: Any, found_catalogs: Any, streams_to_select: Any, select_all_fields: bool = True
    ):
        """Perform table and field selection based off of the streams to select
        set and field selection parameters.

        Verify this results in the expected streams selected and all or
        no fields selected for those streams.
        """
        # Select all available fields or select no fields from all testable streams
        exclude_streams = self.expected_streams().difference(streams_to_select)
        self.select_all_streams_and_fields(
            conn_id=conn_id,
            catalogs=found_catalogs,
            select_all_fields=select_all_fields,
            exclude_streams=exclude_streams,
        )

        catalogs = menagerie.get_catalogs(conn_id)

        # Ensure our selection worked
        for cat in catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat["stream_id"])
            # Verify all testable streams are selected
            selected = catalog_entry.get("annotated-schema").get("selected")
            if cat["stream_name"] not in streams_to_select:
                self.assertFalse(selected, msg="Stream selected, but not testable.")
                continue  # Skip remaining assertions if we aren't selecting this stream
            self.assertTrue(selected, msg="Stream not selected.")

            if select_all_fields:
                # Verify all fields within each selected stream are selected
                for field, field_props in catalog_entry.get("annotated-schema").get("properties").items():
                    field_selected = field_props.get("selected")
                    self.assertTrue(field_selected, msg="Field not selected.")
            else:
                # Verify only automatic fields are selected
                expected_automatic_fields = self.expected_automatic_fields().get(cat["tap_stream_id"])
                selected_fields = self.get_selected_fields_from_metadata(catalog_entry["metadata"])
                self.assertEqual(expected_automatic_fields, selected_fields)

    def expected_schema_keys(self, stream: Any):
        props = self._load_schemas(stream).get(stream).get("properties")
        if not props:
            props = self._load_schemas(stream, shared=True).get(stream).get("properties")

        assert props, "schema not configured proprerly"

        return props.keys()

    @staticmethod
    def _get_abs_path(path: str):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

    def _load_schemas(self, stream, shared: bool = False):
        schemas = {}

        file_name = "shared/" + stream[:-1] + ".json" if shared else stream + ".json"
        path = self._get_abs_path("schemas") + "/" + file_name
        final_path = path.replace("tests", self.tap_name().replace("-", "_"))

        with open(final_path) as file:
            schemas[stream] = json.load(file)

        return schemas

    def create_interrupt_sync_state(self, state: dict, interrupt_stream: str, pending_streams: list, sync_records: Any):
        """Creates a state for simulating a interrupted sync and backdating
        bookmarks for interrupted stream."""

        interrupted_sync_states = copy.deepcopy(state)
        bookmark_state = interrupted_sync_states["bookmarks"]
        # Set the interrupt stream as currently syncing
        interrupted_sync_states["currently_syncing"] = interrupt_stream

        # Remove bookmark for completed streams to set them as pending
        # Setting value to start date wont be needed as all other streams are full_table
        for stream in pending_streams:
            bookmark_state.pop(stream, None)

        # update state for chats stream and set the bookmark to a date earlier
        chats_bookmark = bookmark_state.get("chats", {})
        chats_bookmark.pop("offset", None)
        chats_rec, offline_msgs_rec = [], []
        for record in sync_records.get("chats").get("messages"):
            if record.get("action") == "upsert":
                rec = record.get("data")
                if rec["type"] == "offline_msg":
                    offline_msgs_rec.append(rec)
                else:
                    chats_rec.append(rec)

        # set a deferred bookmark value for both the bookmarks of chat stream
        chat_index = len(chats_rec) // 2 if len(chats_rec) > 1 else 0
        chats_bookmark["chat.end_timestamp"] = chats_rec[chat_index]["end_timestamp"]

        msg_index = len(offline_msgs_rec) // 2 if len(offline_msgs_rec) > 1 else 0
        chats_bookmark["offline_msg.timestamp"] = offline_msgs_rec[msg_index]["timestamp"]

        bookmark_state["chats"] = chats_bookmark
        interrupted_sync_states["bookmarks"] = bookmark_state
        return interrupted_sync_states
