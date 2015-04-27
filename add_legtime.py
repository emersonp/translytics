# Copyright (c) 2015 Parker Harris Emerson

import postgresql
import pprint
import psycopg2
import psycopg2.extras
import re
import sys
from operator import itemgetter

import route
from route import *

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

# Fetches the first row with an empty leg time.
def fetchEmptyLegTime():
    cursor.execute('SELECT * FROM {} WHERE leg_time IS NULL LIMIT 1'.format(table))
    row = cursor.fetchall()
    return row

# Calculates and adds leg_time for each leg in a trip.
def calcEmptyLegTime(trip):
    sortedTrip = sorted(trip, key = lambda stop: (stop['leave_time']))
    for index in range(len(sortedTrip) - 1):
        sortedTrip[index]['leg_time'] = sortedTrip[index + 1]['leave_time'] - \
                sortedTrip[index]['leave_time']
    sortedTrip[len(sortedTrip) - 1]['leg_time'] = -1
    return sortedTrip

def main():
    establishConnection()
    route.connection = connection
    route.cursor = cursor

    firstRow = fetchEmptyLegTime()[0]
    printRow(firstRow)
    sequenceNumber = calcSequenceNumber(firstRow)
    trip = fetchTripFromSequence(sequenceNumber)
    sortedTrip = calcEmptyLegTime(trip)
    for stop in sortedTrip:
        print(stop['leg_time'])

    if connection:
        connection.close()
    
if __name__ == "__main__":
    main()
