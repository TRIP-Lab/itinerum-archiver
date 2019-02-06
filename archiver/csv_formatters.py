#!/usr/bin/env python
# Kyle Fitzsimmons, 2017-2019
import csv
from datetime import datetime
from decimal import Decimal
import os
import pytz
import time


def _format_UTC_timestamp(ts):
    if ts:
        return ts.astimezone(pytz.utc).strftime('%Y-%m-%d %H:%M:%S')


def survey_response_header(mobile_users_cols, questions_cols, timestamp_cols, location_cols, exclude_cols):
    header = mobile_users_cols + questions_cols

    # add location latitude/longitude as two separate columns
    for col in location_cols:
        lat_col = col + '_lat'
        lon_col = col + '_lon'
        header.append(lat_col)
        header.append(lon_col)

    # add timestamp columns as UTC timestamp and epoch time
    for col in timestamp_cols:
        col_idx = header.index(col)
        header[col_idx] = col + '_UTC'
        header.insert(col_idx + 1, col + '_epoch')

    header = [h.replace(' ', '_') for h in header]
    return header


def survey_response_row(header, user, timestamp_cols, location_cols):
    row = []
    user = dict(user)
    user.update(user['response'])

    # process timestamp cols as first cols of each row
    for c in timestamp_cols:
        ts = user.get(c)
        row.append(_format_UTC_timestamp(ts))
        row.append(int(datetime.timestamp(ts)))

    for h in header:
        # skip specially handled timestamp and location columns
        skip_timestamp_header = any([h.startswith(col) for col in timestamp_cols])
        skip_location_header = any([h.startswith(col) for col in location_cols])
        if skip_timestamp_header or skip_location_header:
            continue

        value = user.get(h)
        if isinstance(value, datetime):
            value = value.replace(microsecond=0)
        elif isinstance(value, list):
            str_list = []
            for v in value:
                if isinstance(v, str):
                    v = v.replace(u'\u2019', "'")
                    str_list.append(v)
                else:
                    str_list.append(str(v))
            value = ';'.join(str_list)
        elif isinstance(value, str):
            # replace fancy apostrophe with ascii
            value = value.replace(u'\u2019', "'")
        row.append(value)

    # append location dictionaries as lat/lng pairs of columns at end
    for c in location_cols:
        value = user.get(c)
        if value and isinstance(value, str):
            lat, lon = value.split()
            row.append(lat)
            row.append(lon)
        elif value:
            row.append(value['latitude'])
            row.append(value['longitude'])
        else:
            row.append(None)
            row.append(None)
    return row


def coordinate_row(header, point):
    point['timestamp_UTC'] = _format_UTC_timestamp(point['timestamp_UTC'])

    row = []
    for h in header:
        value = point[h]
        if isinstance(value, Decimal):
            value = float(value)
        if isinstance(value, datetime):
            value = value.replace(microsecond=0).isoformat()
        row.append(value)
    return row


def group_prompt_responses(prompts):
    prompts_by_displayed_at = {}
    for p in prompts:
        p = dict(p)
        prompts_by_displayed_at.setdefault(p['displayed_at_UTC'], []).append(p)

    # get the grouped responses by `displayed_at_UTC` and assign a prompt number
    # TODO: does this group, upgroup and re-group? double-check this.
    labeled_prompts = []
    for displayed_at, prompt_group in sorted(prompts_by_displayed_at.items()):
        by_displayed_at = {}
        for g in prompt_group:
            by_displayed_at.setdefault(g['displayed_at_UTC'], []).append(g)
        for displayed_at, responses in by_displayed_at.items():
            seen = []
            for idx, r in enumerate(responses, start=1):
                answer = r['response']
                if answer not in seen:
                    seen.append(answer)
                    # relabel num as list/array index
                    r['prompt_num'] = idx
                    labeled_prompts.append(r)
    return labeled_prompts


def prompt_response_row(header, response):
    response['displayed_at_UTC'] = _format_UTC_timestamp(response['displayed_at_UTC'])
    response['recorded_at_UTC'] = _format_UTC_timestamp(response['recorded_at_UTC'])
    response['edited_at_UTC'] = _format_UTC_timestamp(response['edited_at_UTC'])

    row = []
    for h in header:
        value = response[h]
        if isinstance(value, Decimal):
            value = float(value)
        if isinstance(value, datetime):
            value = value.replace(microsecond=0).isoformat()
        if isinstance(value, list):
            value = ';'.join(sorted({v for v in value}))
        if isinstance(value, str):
            value = value.replace(u'\u2019', "'")
        row.append(value)
    return row


def cancelled_prompt_row(header, cancelled):
    cancelled['displayed_at_UTC'] = _format_UTC_timestamp(cancelled['displayed_at_UTC'])
    cancelled['cancelled_at_UTC'] = _format_UTC_timestamp(cancelled['cancelled_at_UTC'])
    row = []
    for h in header:
        value = cancelled[h]
        if isinstance(value, Decimal):
            value = float(value)
        elif isinstance(value, datetime):
            value = value.replace(microsecond=0).isoformat()
        row.append(value)
    return row
