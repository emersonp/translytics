# Copyright (c) 2015 Parker Harris Emerson

import postgresql
import pprint
import psycopg2
import psycopg2.extras
import sys

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

# Returns a 'sequence number', a unique bus identifier, from a location_id, a
# route number, and a scheduled stop_time.
def fetchSequenceNumberFromStop(location, route, time):
    cursor.execute('SELECT direction, service_key, trip_number FROM {} WHERE route_number = {} AND location_id = {} AND stop_time = {}'.format(table, route, location, time))
    row = cursor.fetchone()
    sequence = {'route_number': route, 'direction': row['direction'],
            'service_key': row['service_key'], 'trip_number': row['trip_number']}
    return sequence

def fetchTripFromSequence(sequenceNumber):
    cursor.execute('SELECT * FROM {} WHERE route_number = {} AND direction = {} AND service_key = \'{}\' and trip_number = {}'.format(table, sequenceNumber['route_number'],
        sequenceNumber['direction'], sequenceNumber['service_key'],
        sequenceNumber['trip_number']))
    rows = cursor.fetchall()
    return rows

# Parses a 'sequence number', a unique bus identifier, from a location_id, a
# route number, and a scheduled stop_time.
def parseSequenceNumber(row):
    sequence_number = str(row['route_number']) + str(row['direction']) + \
            str(row['service_key']) + str(row['trip_number'])
    sequence = {'route_number': row['route_number'], 'direction': row['direction'],
            'service_key': row['service_key'], 'trip_number': row['trip_number'],
            'sequence_number': sequence_number}
    return sequence
        
def main():
    establishConnection()

    row = fetchMatchParam('leave_time', 18574)
    row = row[0]
    sequence = fetchTripFromSequence(parseSequenceNumber(row))
    sequence = calcSequenceSegment(sequence, 9030, 5009)
    for stop in sequence:
        print(stop['leave_time'], "\t", stop['location_id'])
    print("Max Load: " + str(calcMaxLoad(sequence)))
    
    
    if connection:
        connection.close()
    
    print("\nSuccess!\n")

if __name__ == "__main__":
    main()
