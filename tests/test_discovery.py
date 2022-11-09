"""Test tap discovery."""
import re

from base import BaseTapTest
from tap_tester import connections, menagerie


class TestZendeskChatDiscovery(BaseTapTest):
    @staticmethod
    def name():
        return "tap_tester_tap_zendesk_chat_discovery"

    def test_run(self):
        """
        Testing that discovery creates the appropriate catalog with valid metadata.

        - Verify number of actual streams discovered match expected
        - Verify the stream names discovered were what we expect
        - Verify stream names follow naming convention streams should only have lowercase alphas and underscores
        - verify there is only 1 top level breadcrumb
        - verify primary key(s)
        - verify that primary keys are given the inclusion of automatic.
        - verify that all other fields have inclusion of available metadata.
        """
        conn_id = connections.ensure_connection(self)

        # Verify number of actual streams discovered match expected
        found_catalogs = self.run_and_verify_check_mode(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg=f"unable to locate schemas for connection {conn_id}")
        self.assertEqual(
            len(found_catalogs),
            len(self.expected_streams()),
            msg="Expected {} streams, actual was {} for connection {},"
            " actual {}".format(len(self.expected_streams()), len(found_catalogs), found_catalogs, conn_id),
        )

        # Verify the stream names discovered were what we expect
        found_catalog_names = {c["tap_stream_id"] for c in found_catalogs}
        self.assertEqual(
            set(self.expected_streams()), set(found_catalog_names), msg="Expected streams don't match actual streams"
        )

        # Verify stream names follow naming convention
        # streams should only have lowercase alphas and underscores
        self.assertTrue(
            all([re.fullmatch(r"[a-z_]+", name) for name in found_catalog_names]),
            msg="One or more streams don't follow standard naming",
        )

        for stream in self.expected_streams():
            with self.subTest(stream=stream):
                catalog = next(iter([catalog for catalog in found_catalogs if catalog["stream_name"] == stream]))
                assert catalog  # based on previous tests this should always be found

                schema_and_metadata = menagerie.get_annotated_schema(conn_id, catalog["stream_id"])
                metadata = schema_and_metadata["metadata"]
                schema = schema_and_metadata["annotated-schema"]

                # verify the stream level properties are as expected
                # verify there is only 1 top level breadcrumb
                stream_properties = [item for item in metadata if item.get("breadcrumb") == []]
                self.assertTrue(len(stream_properties) == 1, msg="There is more than one top level breadcrumb")

                # verify replication key(s)
                actual = set(
                    stream_properties[0].get("metadata", {self.REPLICATION_KEYS: []}).get(self.REPLICATION_KEYS) or []
                )
                expected = self.expected_replication_keys()[stream] or set()
                self.assertEqual(actual, expected, msg=f"expected replication key {expected} but actual is {actual}")

                # verify primary key(s)
                self.assertEqual(
                    set(stream_properties[0].get("metadata", {self.PRIMARY_KEYS: []}).get(self.PRIMARY_KEYS, [])),
                    self.expected_primary_keys()[stream],
                    msg="expected primary key {} but actual is {}".format(
                        self.expected_primary_keys()[stream],
                        set(stream_properties[0].get("metadata", {self.PRIMARY_KEYS: None}).get(self.PRIMARY_KEYS, [])),
                    ),
                )

                expected_automatic_fields = self.expected_automatic_fields()[stream] or set()

                # verify that primary and replication keys
                # are given the inclusion of automatic in metadata.

                actual_automatic_fields = {
                    mdata["breadcrumb"][-1]
                    for mdata in metadata
                    if mdata["breadcrumb"] and mdata["metadata"]["inclusion"] == "automatic"
                }

                actual_available_fields = {
                    mdata["breadcrumb"][-1]
                    for mdata in metadata
                    if mdata["breadcrumb"] and mdata["metadata"]["inclusion"] == "available"
                }

                self.assertEqual(
                    expected_automatic_fields,
                    actual_automatic_fields,
                    msg="expected {} automatic fields but got {}".format(
                        expected_automatic_fields, actual_automatic_fields
                    ),
                )

                # verify that all other fields have inclusion of available
                # This assumes there are no unsupported fields for SaaS sources

                self.assertSetEqual(actual_available_fields, set(schema["properties"]) - actual_automatic_fields)
