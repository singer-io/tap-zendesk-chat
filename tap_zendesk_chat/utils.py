#!/usr/bin/env python3
import os
from datetime import datetime, timedelta

import singer
from singer.utils import load_json, strptime_to_utc


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(tap_stream_id):
    path = f"schemas/{tap_stream_id}.json"
    schema = load_json(get_abs_path(path))
    dependencies = schema.pop("tap_schema_dependencies", [])
    refs = {}
    for sub_stream_id in dependencies:
        refs[sub_stream_id] = load_schema(sub_stream_id)
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
