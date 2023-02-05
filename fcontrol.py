import psycopg2
import datetime
from assistfunc import *

__all__ = ["update_flights"]

interval = 1
middleData = datetime.date(2016,9,1)
baseSeqVal = 300000



def update_flights(cursor, connection):
    today = datetime.datetime.now()
    messageLog = MessageL()
    try:
        for day in range(interval):
            messageLog.append(f'Begin update flights: {datetime.datetime.now()}')
            dateCheck = today.date() + datetime.timedelta(days = day)
            messageLog.append(dateCheck)
            cursor.execute('SELECT count(*) FROM flights WHERE scheduled_departure::date = %(date)s',{'date': dateCheck})
            if (cursor.fetchone()[0] == 0):
                # Cheсking the sequence for flight_id
                cursor.execute('SELECT nextval(\'flights_flight_id_seq\')')
                cursor.execute('SELECT currval(\'flights_flight_id_seq\')')
                currentSequence = cursor.fetchone()[0]
                cursor.execute('SELECT max(flight_id) FROM flights')
                maxSequence = cursor.fetchone()[0]
                if currentSequence < maxSequence:
                    cursor.execute(f'SELECT setval(\'flights_flight_id_seq\', {maxSequence})')
                elif currentSequence >= maxSequence and currentSequence < baseSeqVal:
                    cursor.execute(f'SELECT setval(\'flights_flight_id_seq\', {baseSeqVal})')
                # Set date in db for new entries    
                if (dateCheck.month >= middleData.month and dateCheck.day >= middleData.day):
                    olddate = dateCheck.replace(year = 2016)
                else:
                    olddate = dateCheck.replace(year = 2017)
                cursor.execute('SELECT count(*) FROM flights WHERE scheduled_departure::date = %(date)s',{'date': olddate})
                countBefore = cursor.fetchone()[0]
                messageLog.append(f"Number of entries as of {olddate}: {countBefore}")
                # Queryset execution
                cursor.execute('CREATE TEMP TABLE newdata AS SELECT flight_id, flight_no, scheduled_departure, scheduled_arrival, departure_airport, arrival_airport, status, aircraft_code \
                    FROM flights WHERE scheduled_departure::date = %(date)s',{'date': olddate})
                cursor.execute('UPDATE newdata SET flight_id = nextval(\'flights_flight_id_seq\'), \
                            scheduled_arrival = scheduled_arrival::date - scheduled_departure::date + %(date)s::date + scheduled_arrival::time,\
                            scheduled_departure = %(date)s::date + scheduled_departure::time, status = \'Scheduled\'',{'date': dateCheck})
                cursor.execute('INSERT INTO flights (flight_id, flight_no, scheduled_departure, scheduled_arrival, departure_airport, arrival_airport, status, aircraft_code) \
                            SELECT flight_id, flight_no, scheduled_departure, scheduled_arrival, departure_airport, arrival_airport, status, aircraft_code FROM newdata')
                cursor.execute('DROP TABLE newdata')
                cursor.execute('SELECT count(*) FROM flights WHERE scheduled_departure::date = %(date)s',{'date': dateCheck})
                countAfter = cursor.fetchone()[0]
                if ( countAfter != countBefore):
                    raise Exception('Count of entries don`t equal')
                else:
                    messageLog.append(f'Number of entries as of {dateCheck}: {countAfter}')
            else:
                messageLog.append(f'Entries on {dateCheck} already exsist')
    except (Exception, psycopg2.Error) as e:
        connection.rollback()
        messageLog.append(f"Error in flights update: {e}")
    else:
        connection.commit()
        messageLog.append('Execution is committed')
    finally:
        messageLog.append(f'End update flights: {datetime.datetime.now()}')
        with open('out.txt', 'a') as f:
            f.write('\n')
            f.write(messageLog.gets())
        messageLog.clear()