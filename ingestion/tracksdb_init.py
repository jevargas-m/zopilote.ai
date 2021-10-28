import os
import sqlite3

DATABASE_FILENAME = "tracks2.db"

if not os.path.isfile(DATABASE_FILENAME):
    print("ERROR: No db file present")
    exit()

con = sqlite3.connect(DATABASE_FILENAME)
cursor = con.cursor()

sql = """
  DROP TABLE IF EXISTS rawtracks
"""
cursor.execute(sql)

sql = """
  DROP TABLE IF EXISTS files_ingested
"""
cursor.execute(sql)

sql = '''
  create table if not exists rawtracks(
    track_id INTEGER,
    time_begin TEXT,
    time_end TEXT,
    altitude INTEGER,
    altitude_mode INTEGER,
    lat REAL,
    lon REAL
  )
'''
cursor.execute(sql)

sql = '''
  create table if not exists files_ingested(
    filename TEXT UNIQUE,
    track_id INTEGER PRIMARY KEY AUTOINCREMENT,
    ingestion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    parsed_correct BIT DEFAULT 0
  )
'''
cursor.execute(sql)

con.commit()
