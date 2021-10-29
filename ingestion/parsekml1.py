"""
Parse all kml files in staging directory.
This script must run on ingestion directory and looks for files  in
    rawdata/kmlstaging/*.kml

kml type 1 in which flight data follows this pattern:
    				<Placemark>
					<Point>
						<altitudeMode>absolute</altitudeMode>
						<coordinates>-100.199575,18.976367,2643</coordinates>
					</Point>
					<TimeSpan>
						<begin>2021-02-03T18:41:44Z</begin>
						<end>2021-02-03T18:41:45Z</end>
					</TimeSpan>
				</Placemark>
"""

import os
import glob
from bs4 import BeautifulSoup as Soup

from icecream import ic
import dbconn

con = dbconn.connect()

COMMIT_AFTER_N_PLACEMARKS = 1000


cursor = con.cursor()


def register_file_in_db(filename):
    sql = """ INSERT INTO files_ingested (filename) \
                        VALUES (%s)
    """
    cursor.execute(sql, (filename,))
    con.commit()

    sql = """ SELECT track_id FROM files_ingested WHERE filename = %s
    """
    cursor.execute(sql, (filename,))
    return cursor.fetchone()[0]

def parse_kmlfile(filename, track_id):
    with open(filename) as data:
        kml_soup = Soup(data, 'lxml-xml')

    placemarks = kml_soup.find_all('Placemark')
    error_counter = 0
    pm_counter = 1
    for pm in placemarks:
        try:
            if pm.Point is not None and pm.TimeSpan is not None:
                # this flags a flight point
                if pm.Point.coordinates is not None:
                    alt_mode = pm.Point.find('altitudeMode').contents[0]
                    alt_mode = 1 if alt_mode == 'absolute' else 0
                    coords_str = pm.Point.find('coordinates').contents[0]
                    coords = coords_str.split(",")
                    lon = coords[0]
                    lat = coords[1]
                    alt = coords[2]
                    coord_geom = f'POINT({lon} {lat})'
                    time_begin = pm.TimeSpan.find('begin').contents[0]
                    time_end = pm.TimeSpan.find('end').contents[0]

                    sql = """
                        INSERT INTO rawtracks (track_id, time_begin, time_end, \
                            altitude, altitude_mode, coords) \
                        VALUES (%s, %s, %s, %s, %s, ST_GeometryFromText(%s))
                    """
                    values = (track_id, time_begin, time_end, alt, alt_mode, coord_geom)
                    cursor.execute(sql, values)
                else:
                    print('WARNING: Point has timespan but not coords')
        except:
            error_counter += 1
            if error_counter > 3:
                print('WARNING: Could not parse file')
                return False
        pm_counter += 1
        if pm_counter % COMMIT_AFTER_N_PLACEMARKS == 0:
            con.commit()
    con.commit()
    return True


for filename in glob.glob('ingestion/rawdata/kmlstaging/*.kml'):
    ic(filename)
    track_id = register_file_in_db(filename.split("/")[-1])
    ic(track_id)
    is_parsed = parse_kmlfile(filename, track_id)
    if is_parsed:
        cursor.execute("""
            UPDATE files_ingested 
            SET parsed_correct = TRUE 
            WHERE track_id = %s
        """, (track_id,))
 #       os.remove(filename)
    else:
        cursor.execute("DELETE FROM rawtracks WHERE track_id = %s", (track_id,))
    con.commit()

    print(filename + ' ** DONE **')
        

