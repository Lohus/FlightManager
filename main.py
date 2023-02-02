import datetime
import psycopg2
from psycopg2 import Error

interval = 1
middleData = datetime.date(2016,9,1)
baseSeqVal = 300000


# Initialization
try:
     conn = psycopg2.connect(dbname = 'djangodb',
                    user = 'djangomanager',
                    password = 'managerpass',
                    host = 'localhost',
                    port = '5432')
     cur = conn.cursor()
except (Exception, Error) as e:
     print(f"Initialization error: {e}")
else:
     try:
          
          today = datetime.datetime.now()
          for day in range(interval):
               dateCheck = today.date() + datetime.timedelta(days = day)
               print(dateCheck)
               cur.execute('SELECT count(*) FROM flights WHERE scheduled_departure::date = %(date)s',{'date': dateCheck})
               if (cur.fetchone()[0] == 0):
                    cur.execute('SELECT nextval(\'flights_flight_id_seq\')')
                    cur.execute('SELECT currval(\'flights_flight_id_seq\')')
                    currentSequence = cur.fetchone()[0]
                    cur.execute('SELECT max(flight_id) FROM flights')
                    maxSequence = cur.fetchone()[0]
                    if currentSequence < maxSequence:
                         cur.execute(f'SELECT setval(\'flights_flight_id_seq\', {maxSequence})')
                    elif currentSequence > baseSeqVal and baseSeqVal < 300000:
                         cur.execute(f'SELECT setval(\'flights_flight_id_seq\', {baseSeqVal})')
                    if (dateCheck.month >= middleData.month and dateCheck.day >= middleData.day):
                         olddate = dateCheck.replace(year = 2016)
                    else:
                         olddate = dateCheck.replace(year = 2017)
                    cur.execute('SELECT count(*) FROM flights WHERE scheduled_departure::date = %(date)s',{'date': olddate})
                    tempcountbefore = cur.fetchone()[0]
                    print( olddate, tempcountbefore)
                    cur.execute('CREATE TEMP TABLE newdata AS SELECT flight_id, flight_no, scheduled_departure, scheduled_arrival, departure_airport, arrival_airport, status, aircraft_code \
                         FROM flights WHERE scheduled_departure::date = %(date)s',{'date': olddate})
                    cur.execute('UPDATE newdata SET flight_id = nextval(\'flights_flight_id_seq\'), \
                         scheduled_arrival = scheduled_arrival::date - scheduled_departure::date + %(date)s::date + scheduled_arrival::time,\
                         scheduled_departure = %(date)s::date + scheduled_departure::time, status = \'Scheduled\'',{'date': dateCheck})
                    cur.execute('INSERT INTO flights (flight_id, flight_no, scheduled_departure, scheduled_arrival, departure_airport, arrival_airport, status, aircraft_code) \
                         SELECT flight_id, flight_no, scheduled_departure, scheduled_arrival, departure_airport, arrival_airport, status, aircraft_code FROM newdata')
                    cur.execute('DROP TABLE newdata')
                    conn.commit()
                    print('Data added in flights table')
     except Exception as error:
          print(f"Problem when working with PostgresSQL: {error}")
finally:
     if conn:
          cur.close()
          conn.close()
          if not conn: print("Work with PostgresSQL is completed")
