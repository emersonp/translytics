# Copyright (c) 2015 Parker Harris Emerson

import postgresql
import pprint
import psycopg2
import psycopg2.extras
import re
import sys
from operator import itemgetter

connection = None
cursor = None
table = 'stopdata_03122014'
pp = pprint.PrettyPrinter(indent=4)

def establishConnection():

    try:
        global connection, cursor
        connection = psycopg2.connect(database='capstone_db', user='parker')
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

    except psycopg2.DatabaseError as e:
        print("Database Error: ", e.value)
        sys.exit(1)

# Calculates max passenger load of a sequence
def calcMaxLoad(sequence):
    maxRow = None
    maxLoad = 0
    for row in sequence:
        if row['estimated_load'] > maxLoad:
            maxRow = row
            maxLoad = row['estimated_load']
    return maxLoad

def calcSequenceNumber(row):
    return fetchSequenceNumberFromStop(row['location_id'], row['route_number'], row['stop_time'])

# Filters sequence based on start stop and end stop.
def calcSequenceSegment(sequence, startStop, endStop):
    startTime = 0
    endTime = 0
    newSequence = []
    for stop in sequence:
        if stop['location_id'] == startStop:
            startTime = stop['stop_time']
        if stop['location_id'] == endStop:
            endTime = stop['stop_time']
    newSequence = [i for i in sequence if i['stop_time'] >= startTime and \
            i['stop_time'] <= endTime]
    return newSequence

# Fetches all rows where a param matches a value.
def fetchMatchParam(param, value):
    cursor.execute('SELECT * FROM {} WHERE {}={}'.format(table, param, value))
    rows = cursor.fetchall()
    return rows

# Fetches all rows where a param is between two values.
def fetchBetweenParam(param, minValue, maxValue):
    cursor.execute('SELECT * FROM {} WHERE {} BETWEEN {} AND {}'.format(table, param, minValue, maxValue))
    rows = cursor.fetchall()
    return rows

# As fetchBetweenParam, but limited to one route.
def fetchRouteBetweenParam(route, param, minValue, maxValue):
    cursor.execute('SELECT * FROM {} WHERE route_number = {} AND {} BETWEEN {} AND {}'.format(table, route, param, minValue, maxValue))
    rows = cursor.fetchall()
    return rows

# Fetches all rows where a route stops at a location between two times.
def fetchRouteStopWindow(route, location, startTime, endTime):
    cursor.execute('SELECT * FROM {} WHERE route_number = {} AND location_id = {} AND stop_time BETWEEN {} AND {}'.format(table, route, location, startTime, endTime))
    rows = cursor.fetchall()
    return rows

# Returns a 'sequence number', a unique bus identifier, from a location_id, a
# route number, and a scheduled stop_time.
def fetchSequenceNumberFromStop(location, route, time):
    cursor.execute('SELECT direction, service_key, trip_number FROM {} WHERE route_number = {} AND location_id = {} AND stop_time = {}'.format(table, route, location, time))
    row = cursor.fetchone()
    sequence = {'route_number': route, 'direction': row['direction'],
            'service_key': row['service_key'], 'trip_number': row['trip_number']}
    return sequence

# Returns every row representing a sequence.
def fetchTripFromSequence(sequenceNumber):
    cursor.execute('SELECT * FROM {} WHERE route_number = {} AND direction = {} AND service_key = \'{}\' and trip_number = {}'.format(table, sequenceNumber['route_number'],
        sequenceNumber['direction'], sequenceNumber['service_key'],
        sequenceNumber['trip_number']))
    rows = cursor.fetchall()
    return rows

# Parses a 'sequence number', a unique bus identifier, from a location_id,
# a route number, and a scheduled stop_time.
def parseSequenceNumber(row):
    sequence_number = str(row['route_number']) + str(row['direction']) + \
            str(row['service_key']) + str(row['trip_number'])
    sequence = {'route_number': row['route_number'], 'direction': row['direction'],
            'service_key': row['service_key'], 'trip_number': row['trip_number'],
            'sequence_number': sequence_number}
    return sequence

# Parses a time string of format "HH:MM" and returns an int of seconds.
def parseTimeToSeconds(string):
    regTime = re.split(r"[:,]", string)
    return int(regTime[0]) * 3600 + int(regTime[1]) * 60

# Converts an int of seconds into a string of time of format "HH:MM"
def parseSecondsToTime(int):
    hours = 0
    minutes = 0
    seconds = int
    while seconds >= 3600:
        hours += 1
        seconds -= 3600
    while seconds > 60:
        minutes += 1
        seconds -= 60
    return str(hours) + ":" + '{0:02}'.format(minutes)

def printMaxLoadList(route, startStop, endStop, startTime, endTime):
    sequence_numbers = []
    trips = []
    test = 0
    departures = fetchRouteStopWindow(route, startStop, startTime, endTime)
    for stop in departures:
        sequence_numbers.append(parseSequenceNumber(stop))
    for sequence_number in sequence_numbers:
        segment = calcSequenceSegment(fetchTripFromSequence(sequence_number), startStop, endStop)
        sortedSeg = sorted(segment, key = lambda stop: (stop['stop_time']))
        trips.append({'stop_time': sortedSeg[0]['stop_time'],
            'max_load': calcMaxLoad(sortedSeg)})
        if not test:
            printRow(segment[0])
            test += 1

    sortedTrips = sorted(trips, key = lambda stop: (stop['stop_time']))
    print("\nSTOP TIME\t MAX LOAD")
    for trip in sortedTrips:
        print(parseSecondsToTime(trip['stop_time']), "|\t\t", trip['max_load'])

# Pretty prints a row of data.
def printRow(row):
    print("Row ID:", row['id'])
    print("Service Data:", row['service_date'])
    print("Vehicle Number:", row['vehicle_number'])
    print("Leave Time:", row['leave_time'])
    print("Train:", row['train'])
    print("Badge:", row['badge'])
    print("Route Number:", row['route_number'])
    print("Direction:", row['direction'])
    print("Service Key:", row['service_key'])
    print("Trip Number:", row['trip_number'])
    print("Stop Time:", row['stop_time'])
    print("Arrive Time:", row['arrive_time'])
    print("Dwell:", row['dwell'])
    print("Location ID:", row['location_id'])
    print("Door:", row['door'])
    print("Lift:", row['lift'])
    print("Ons:", row['ons'])
    print("Offs:", row['offs'])
    print("Estimated Load:", row['estimated_load'])
    print("Maximum Speed:", row['maximum_speed'])
    print("Train Mileage:", row['train_mileage'])
    print("Pattern Distance:", row['pattern_distance'])
    print("Location Distance:", row['location_distance'])
    print("X Coordinate:", row['x_coordinate'])
    print("Y Coordinate:", row['y_coordinate'])
    print("Data Source:", row['data_source'])
    print("Schedule Status:", row['schedule_status'])
    print("Leg Time:", row['leg_time'])
    print("")

        
def main():
    establishConnection()

    if len(sys.argv) == 6:
        userRoute = int(sys.argv[1])
        userDepartStop = int(sys.argv[2])
        userArriveStop = int(sys.argv[3])
        userStartTime = parseTimeToSeconds(sys.argv[4])
        userEndTime = parseTimeToSeconds(sys.argv[5])

        printMaxLoadList(userRoute, userDepartStop, userArriveStop, \
            userStartTime, userEndTime)
    else:
        print("Please use format ./route.py [Route] [StartStop] [EndStop]",
                "[StartTime HH:MM] [EndTime]")
        print("\n\te.g., ./route.py 15 9030 5009 14:30 16:30\n\n")

    if connection:
        connection.close()
    
if __name__ == "__main__":
    main()
