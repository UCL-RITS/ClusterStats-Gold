# SQlite queries for the Gold database.
# The values are inserted at the ? by cursor.execute(query, (val1, val2, val3))

# Get allocation usage data for all institutes for all time.
# Order by allocation start, end and then project name 
# (as first allocation all starts on same date).
def gold_by_all_allocation_periods():
    query = ("""SELECT j.g_project, sum(j.g_charge), r.g_id as date_alloc, r.g_account,
                       a.g_start_time, a.g_end_time, count(*) as num_jobs
                FROM g_job AS j
                INNER JOIN g_reservation_allocation AS r
                  ON j.g_request_id = r.g_request_id
                INNER JOIN g_allocation AS a
                  ON date_alloc = a.g_id
                GROUP BY date_alloc 
                ORDER BY a.g_start_time, a.g_end_time, j.g_project""")
    return query

# Get data for all institutes with an allocation that begins in a given time period.
def gold_by_allocation_start_period():
    query = ("""SELECT j.g_project, sum(j.g_charge), r.g_id as date_alloc, r.g_account,
                       a.g_start_time, a.g_end_time, count(*) as num_jobs
                FROM g_job AS j
                INNER JOIN g_reservation_allocation AS r
                  ON j.g_request_id = r.g_request_id
                INNER JOIN g_allocation AS a
                  ON date_alloc = a.g_id
                WHERE a.g_start_time >= ?
                  AND a.g_start_time <= ?
                GROUP BY date_alloc 
                ORDER BY a.g_start_time, a.g_end_time, j.g_project""")
    return query

# Get data for all institutes with an allocation that begins on this exact date.
def gold_by_allocation_start_date():
    query = ("""SELECT j.g_project, sum(j.g_charge), r.g_id as date_alloc, r.g_account,
                       a.g_start_time, a.g_end_time, count(*) as num_jobs
                FROM g_job AS j
                INNER JOIN g_reservation_allocation AS r
                  ON j.g_request_id = r.g_request_id
                INNER JOIN g_allocation AS a
                  ON date_alloc = a.g_id
                WHERE a.g_start_time = ?
                GROUP BY date_alloc 
                ORDER BY a.g_end_time, j.g_project""")
    return query

