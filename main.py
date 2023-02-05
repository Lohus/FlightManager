import datetime
import psycopg2
from data import *
from fcontrol import *


def main():
# Initialization
     try:
          conn = psycopg2.connect(**serverdata)
          cur = conn.cursor()
     except (Exception, psycopg2.Error) as e:
          print(f"Initialization error: {e}")
     else:
          try:
               update_flights(cur, conn)
          except (Exception, psycopg2.Error) as e:
               print(f"Problem when working with PostgresSQL: {e}")
     finally:
          if conn:
               cur.close()
               conn.close()
               if not conn: print("Work with PostgresSQL is completed")

if __name__ == "__main__":
     main()
