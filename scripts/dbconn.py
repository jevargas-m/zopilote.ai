import configparser
import psycopg2

config = configparser.ConfigParser()
config.read('../config.ini')

def connect():
    return psycopg2.connect(host=config['psql']['host'],
                            port=config['psql']['port'],
                            user=config['psql']['user'],
                            password=config['psql']['password'],
                            database=config['psql']['dbname']
    )

