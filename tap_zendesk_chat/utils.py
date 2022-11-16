#!/usr/bin/env python3
from datetime import datetime, timedelta
from pathlib import Path

import singer
from singer.utils import load_json, strptime_to_utc


def load_schema(tap_stream_id):
    schema = load_json(Path(__file__).parent.resolve() / f"schemas/{tap_stream_id}.json")
    dependencies = schema.pop("tap_schema_dependencies", [])
    refs = {ref: load_schema(ref) for ref in dependencies}
    if refs:
        singer.resolve_schema_references(schema, refs)
    return schema


def break_into_intervals(days, start_time: str, now: datetime):
    delta = timedelta(days=days)
    start_dt = strptime_to_utc(start_time)
    while start_dt < now:
        end_dt = min(start_dt + delta, now)
        yield start_dt, end_dt
        start_dt = end_dt
