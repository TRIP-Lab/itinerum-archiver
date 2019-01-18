import threading


# Database method
def fetch_coordinates_bulk(survey_id=None, last_id=None, chunk_size=None):
    sql = '''SELECT subquery.id, mobile_users.uuid, subquery.latitude, subquery.longitude,
                    subquery.altitude, subquery.speed, subquery.direction,
                    subquery.h_accuracy, subquery.v_accuracy, subquery.acceleration_x,
                    subquery.acceleration_y, subquery.acceleration_z, subquery.mode_detected,
                    subquery.point_type, subquery.timestamp AS "timestamp_UTC"
             FROM (
                   SELECT *
                   FROM mobile_coordinates
                   WHERE mobile_coordinates.survey_id={survey_id}
                   AND mobile_coordinates.id > {id_offset}
                   LIMIT {limit}
             ) subquery
             JOIN mobile_users ON subquery.mobile_id = mobile_users.id;'''
    return DB.query(sql.format(survey_id=survey_id, id_offset=last_id, limit=chunk_size))


# Formatter method
def dump_coordinates_bulk(out_subdir, survey_id=None, chunk_size=1000):
    def _threaded_writer(db_rows, headers, csv_label):
        csv_rows = [headers]
        for point in coordinates_chunk:
            last_coordinates_id = point['id']
            if int(point['latitude']) == 0 and int(point['longitude'] == 0):
                continue

            point['timestamp_epoch'] = point['timestamp_UTC'].timestamp() if point['timestamp_UTC'] else None
            point['timestamp_UTC'] = _format_UTC_timestamp(point['timestamp_UTC'])

            row = []
            for h in headers:
                value = point[h]
                if isinstance(value, Decimal):
                    value = float(value)
                if isinstance(value, datetime):
                    value = value.replace(microsecond=0).isoformat()
                row.append(value)
            csv_rows.append(row)
        filename = 'coordinates-{label}.csv'.format(label=csv_label)
        filepath = os.path.join(out_subdir, filename)
        write_csv(filepath, csv_rows)               

    print('Dumping coordinates to .csv in batched queries...')
    headers = ['uuid', 'latitude', 'longitude', 'altitude', 'speed', 'direction',
               'h_accuracy', 'v_accuracy', 'acceleration_x', 'acceleration_y', 'acceleration_z',
               'mode_detected', 'point_type', 'timestamp_UTC', 'timestamp_epoch']
    timings = []
    idx = 1
    last_seen_coordinates_id = -1
    while True:
        t0 = time.time()
        
        coordinates_chunk = database.fetch_coordinates_bulk(survey_id,
                                                            last_id=last_seen_coordinates_id,
                                                            chunk_size=chunk_size)
        coordinates_chunk = list(coordinates_chunk)
        if not coordinates_chunk:
            break
        last_coordinates_id = coordinates_chunk[-1]['id']
        
        t1 = time.time()
        timings.append(t1 - t0)
        print('Num: {n} / Query: {q:.2f}s / Avg: {a:.2f}s'.format(n=idx,
                                                                  q=timings[-1],
                                                                  a=sum(timings) / len(timings)))
        idx += 1

        t = threading.Thread(target=_threaded_writer, args=(coordinates_chunk, headers, last_coordinates_id))
        t.daemonize = True
        t.start()
        if last_seen_coordinates_id == last_coordinates_id:
            break
        else:
            last_seen_coordinates_id = last_coordinates_id
    t.join()

