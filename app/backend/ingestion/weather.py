
from weatherbit.api import Api
import psycopg2
api_key = "bb90f8e384014e01ab78ea141c13cc68"

api = Api(api_key)

def createConnection(host, database, user, password):
    """
    Create a connection to the database
    """
    conn = psycopg2.connect(host=host,database=database,user=user,password=password)
    return conn



api.set_granularity('hourly')
cities = ['Atlanta', 'Jersey City', 'Milwaukee', 'Tulsa', 'New York', 'San Jose', 'Cleveland', 'Indianapolis', 'Dallas', 'Detroit', 'Bangalore', 'Miami', 'Wichita', 'Los Angeles', 'Oklahoma City', 'Minneapolis', 'Albuquerque', 'Philadelphia', 'Phoenix', 'Austin', 'St. Louis', 'Portland', 'Houston', 'San Francisco', 'Omaha', 'Orlando', 'Kansas City', 'Tampa', 'Las Vegas', 'Colorado Springs', 'Washington', 'Chicago', 'Seattle', 'Cincinnati', 'Nashville', 'Columbus', 'Buffalo', 'New Orleans', 'Denver', 'Virginia Beach', 'Baltimore', 'Charlotte', 'Louisville', 'Pittsburgh', 'San Diego', 'Boston']

for city in cities:
    forecast = api.get_forecast(city='city', hours=24)
    results = forecast.get_series(['temp', 'precip', 'wind_spd', 'wind_dir','rh'])
    
    inserts = []
    for result in results:
        date_hour = result['datetime']
        inserts.append((date_hour,city, result['temp'], result['precip'], result['wind_spd'], result['wind_dir'], result['rh']))
    sql_statement = """INSERT INTO weather_forecast_hourly (date_hour, city, temp, precip, wind_spd, wind_dir, rh) VALUES (%s, %s, %s, %s, %s, %s, %s)"""

    try:
        conn = createConnection(host = '34.122.213.143', database = 'tweeper', user = 'postgres', password = 'aruba')
        cursor = conn.cursor()
        cursor.executemany(sql_statement, inserts)
        conn.commit()
        print("Inserted data for {}".format(city))
    except Exception as e:
        print(e)
        conn.rollback()
        print("Failed to insert data for {}".format(city))