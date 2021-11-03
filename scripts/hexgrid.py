import sys
import collections
import math
from decimal import *
from icecream import ic

import dbconn

Point = collections.namedtuple('Point', ['x', 'y'])
COMMIT_EVERY_UPDATES = 100


def pointy_hex_corner(center, size, i):
    angle_deg = 60 * i - 30
    angle_rad = math.pi / 180 * angle_deg
    return Point(center.x + size * math.cos(angle_rad),
                 center.y + size * math.sin(angle_rad))


def create_hexagon(center, size):
    """https://www.redblobgames.com/grids/hexagons/"""
    return [pointy_hex_corner(center, size, i) for i in range(6)]
    

def create_row(start, size, n_hexagons):
    output = []
    h_spacing = math.sqrt(3) * size
    for i in range(n_hexagons):
        center = Point(x=start.x + h_spacing * i, y=start.y)
        output.append(create_hexagon(center, size))
    return output


def create_grid(start, size, n_rows, n_cols):
    output = []
    v_spacing = 2 * 3 / 4 * size  #width = 2 * size
    for r in range(n_rows):
        row_start = Point(x=start.x, y=start.y + v_spacing * r)
        if r % 2 != 0:
            """odd rows offset"""
            row_start = Point(x=start.x - math.sqrt(3) * size / 2,  y=start.y + v_spacing * r)
        output.append(create_row(row_start, size, n_cols))
    return output


def initialize_table_in_db(con):
    cursor = con.cursor()
    cursor.execute("DROP TABLE IF EXISTS hexgrid")
    sql = """
        CREATE TABLE hexgrid (
            hexagon_id SERIAL PRIMARY KEY,
            hexagon geometry(Polygon, 4326)
        )
    """
    cursor.execute(sql)
    con.commit()


def store_hexagon_in_db(cursor, hexagon):
    sql = """
       INSERT INTO hexgrid (hexagon) VALUES (
       ST_GeometryFromText('POLYGON((%s %s, %s %s, %s %s, %s %s, %s %s, %s %s, %s %s))')) 
    """
    values = [c for corner in hexagon for c in corner]
    values += values[:2]  #close the polygon
    cursor.execute(sql, values)

con = dbconn.connect()

arguments = sys.argv
if len(arguments) != 6:
    print('ERROR: usage hexgrid.py <x1 y1 x2 y2 num_hex_x>')
    exit()
ic(arguments)
x1 = float(arguments[1])
y1 = float(arguments[2])
x2 = float(arguments[3])
y2 = float(arguments[4])
num_hex_x = int(arguments[5])

width = abs(x2 - x1)
height = abs(y2 - y1)
radius = width / num_hex_x / 2
n_cols = math.ceil(width / (2 * radius))
n_rows = math.ceil(height / (2 * radius))

initialize_table_in_db(con)

start = Point(x=x1, y=y1)
hexgrid = create_grid(start, radius, n_rows, n_cols)

cursor = con.cursor()
counter = 1
for row in hexgrid:
    for hexagon in row:
        store_hexagon_in_db(cursor, hexagon)
        if counter % COMMIT_EVERY_UPDATES == 0:
            con.commit()
        counter += 1
con.commit()






