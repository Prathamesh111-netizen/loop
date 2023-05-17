import os
import psycopg2
import requests
import csv
from flask import Flask
from datetime import datetime


app = Flask(__name__)
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="postgrespw",
    port="32768")


def get_data(request_timestamp):
    cur = conn.cursor()

    # get all rows from polls with timestamp >= request_timestamp - 1 week
    cur.execute(
        "SELECT * FROM polls WHERE timestamp_utc >= %s - INTERVAL '1 week' and timestamp_utc <= %s;", (request_timestamp, request_timestamp,))
    rows = cur.fetchall()

    # Store results of each store in a dictionary
    store_results = {}
    for row in rows:
        if row[0] not in store_results:
            store_results[row[0]] = []
        store_results[row[0]].append(row)

    # get timezones for each store
    cur.execute("SELECT * FROM store_timezones;")
    rows = cur.fetchall()
    store_timezones = {}
    for row in rows:
        if row[0] not in store_timezones:
            store_timezones[row[0]] = row[1]

    for row in store_results.keys():
        if row not in store_timezones:
            store_timezones[row] = 'America/Chicago'

    # get business hours for each store
    cur.execute("SELECT * FROM business_hours;")
    rows = cur.fetchall()
    business_hours = {}
    for row in rows:
        if row[0] not in business_hours:
            business_hours[row[0]] = []
        business_hours[row[0]].append(row)

    return store_results, store_timezones, business_hours


def process_data(report_id, request_timestamp):
    store_results, store_timezones, business_hours = get_data(
        request_timestamp)

    # for each store_id
    results = [['store_id', 'uptime_last_hour(in minutes)', 'uptime_last_day(in hours)', 'update_last_week(in hours)',
                'downtime_last_hour(in minutes)', 'downtime_last_day(in hours)', 'downtime_last_week(in hours)']]

    for store in store_results.keys():

        business_hours_for_store = {}
        if store in business_hours:
            for business_hour in business_hours[store]:
                # Create datetime objects with a common date and the time values
                current_date = datetime.now().date()
                datetime1 = datetime.combine(current_date, business_hour[3])
                datetime2 = datetime.combine(current_date, business_hour[2])
                time_difference = datetime1 - datetime2
                hours_difference = time_difference.total_seconds() / 3600

                business_hours_for_store[business_hour[1]] = hours_difference

        total_business_hours = 0
        for i in range(0, 7):
            if i not in business_hours_for_store:
                business_hours_for_store[i] = 24
            total_business_hours += business_hours_for_store[i]

        # find the day of week from request_timestamp
        currday = request_timestamp.weekday() + 7

        # calculation for last week
        uptime_last_week = 0
        uptime_last_day = 0
        uptime_last_hour = 0

        checked = []
        for i in range(0, 7):
            checked.append(False)

        for polls in store_results[store]:
            # calculate day difference between request_timestamp and last timestamp in store_results[store]
            days = (request_timestamp - polls[2]).days
            tday = currday - days
            if tday >= 7:
                tday -= 7
            elif tday < 0:
                tday += 7

            if checked[tday] == False:
                uptime_last_week += business_hours_for_store[tday]
                checked[tday] = True
                if tday == 0:
                    uptime_last_hour += 60
                if tday == 1:
                    uptime_last_day += business_hours_for_store[tday]

        downtime_last_week = total_business_hours - uptime_last_week
        downtime_last_hour = 60 - uptime_last_hour
        downtime_last_day = business_hours_for_store[1] - uptime_last_day

        result = [store, uptime_last_hour, uptime_last_day, uptime_last_week,
                  downtime_last_hour, downtime_last_day, downtime_last_week]

        results.append(result)

    # store results in report_data in csv format
    report_file_location = f"report_{report_id}.csv"

    # Write the data to the CSV file
    with open(report_file_location, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(results)

    print(f"Data has been written to {report_file_location} successfully.")

    return "Complete", report_file_location


def generate_report(report_id):
    cur = conn.cursor()

    # Original flow
    # get report with report_id
    # cur.execute("SELECT * FROM report WHERE report_id = %s;", (report_id,))
    # request_timestamp = cur.fetchall()[0][1]
    # print("request_timestamp ", request_timestamp)

    # Development test flow
    # String representation of the timestamp
    timestamp_string = "2023-01-24 09:06:42.605777"
    # Convert the string to a timestamp
    request_timestamp = datetime.strptime(
        timestamp_string, "%Y-%m-%d %H:%M:%S.%f")

    # get_data
    report_status, report_file_location = process_data(
        report_id, request_timestamp)

    # update report status to Complete and request_file_location to null
    if report_status == "Complete":
        cur.execute(
            "UPDATE report SET report_status = 'Complete', report_file_location = %s WHERE report_id = %s;", (report_file_location, report_id,))
        conn.commit()
        print("report generated")
    else:
        print("error generating report")


@app.route('/get_report/<report_id>')
def get_report(report_id):
    cur = conn.cursor()
    cur.execute("SELECT * FROM report WHERE report_id = %s;", (report_id,))
    rows = cur.fetchall()
    cur.close()
    if len(rows) == 0:
        return 'Report not found'
    else:
        if rows[0][2] == 'Complete':
            return 'Report is ready'
        else:
            return 'Report is not ready'


@app.route('/trigger_report')
def trigger_report():
    cur = conn.cursor()

    # Original flow
    # cur.execute("SELECT * FROM report WHERE request_timestamp >= current_timestamp - INTERVAL '1 hour';")
    # rows = cur.fetchall()
    # report_id = 0
    # if len(rows) == 0:
    #     cur.execute("INSERT INTO report (request_timestamp, request_status) VALUES (current_timestamp, 'Running');")
    #     conn.commit()
    #     cur.execute("SELECT * FROM report WHERE request_timestamp >= current_timestamp - INTERVAL '1 hour';")
    #     rows = cur.fetchall()
    #     report_id = rows[0][0]
    #     generate_report(report_id)
    # else:
    #     report_id = rows[0][0]

    # Development test flow
    conn.commit()
    cur.execute(
        "SELECT * FROM report;")
    rows = cur.fetchall()
    report_id = rows[len(rows)-1][0]

    # Process call to generate report
    generate_report(report_id)
    cur.close()

    return str(report_id)


if __name__ == '__main__':
    app.run()
