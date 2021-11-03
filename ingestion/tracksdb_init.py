import dbconn

con = dbconn.connect()
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
    time_begin TIMESTAMP,
    time_end TIMESTAMP,
    altitude SMALLINT,
    altitude_mode SMALLINT,
    coords GEOMETRY(POINT, 4326)
  )
'''
cursor.execute(sql)

sql = '''
  create table if not exists files_ingested(
    filename TEXT UNIQUE,
    track_id SERIAL PRIMARY KEY,
    ingestion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    parsed_correct BOOLEAN DEFAULT FALSE
  )
'''
cursor.execute(sql)

con.commit()
