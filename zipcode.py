# -*- coding: utf-8 -*-

# encoding=utf8

import os
import sys
import requests
import json
import time
import sqlite3

sys.setrecursionlimit(100000)

sleepTime = 30 #Нэг удаадаа ажиллуулахад маш их хандалт хийх магадлалтай тул үйлдэл хооронд 30 секунд хүлээнэ үү.

db_filename = 'zipdb.db'

db_is_new = not os.path.exists(db_filename)

conn = sqlite3.connect(db_filename)

#table uusgeh function
def create_table(create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        conn.execute(create_table_sql)
    except Error as e:
        print(e)

#herev baaz uuseegui bwal uusgene table uusgene
if db_is_new is True:
    table1 = """ create table zip_location (
                                        id           integer not null,
                                        parent_id     integer,
                                        location_name_mn      text,
                                        location_name_en       text,
                                        zipcode       text,
                                        x       text,
                                        y       text,
                                        category_type_code     text,
                                        category_type_name     text
                                    ); """
 
    table2 = """create table zip_relation (
                    id           integer not null,
                    relation_code      text,
                    zipcode       text,
                    beg_point       text,	
                    end_point       text
                );"""
 
    table3 = """create table zip_coordinate (
                    id           integer not null,
                    zipcode       text,
                    x       text,
                    y       text,
                    num           integer
                );"""

    create_table(table1)
    create_table(table2)
    create_table(table3)

    conn.commit()
    
# data tseverleh
conn.execute("""delete from zip_location;""")
conn.execute("""delete from zip_coordinate;""")
conn.execute("""delete from zip_relation;""")
conn.commit()

cookies = {
    '_ga': 'GA1.2.1612250927.1581611493',
    'PHPSESSID': '1o6mabqq1vmmnv01pdkvgs59i6',
    '_gid': 'GA1.2.489042657.1582862109',
    '_gat': '1',
    'XSRF-TOKEN': 'eyJpdiI6Illsc3ppY1RhbVwvSzBDaGR2VFRqNGRnPT0iLCJ2YWx1ZSI6ImVHUTJsYWJvZVFsXC9BZ1pLbTY0dlIxN25PSnNDeTNZYkd2SnAyUWFMOXVUdkhuYVJaVlhwK0QyRUxiWjkwUjZmT0xodlZLZjIyTWduWXc0V0VCYTNyUT09IiwibWFjIjoiMTNmNjNhYjYxYjI1YzcxZGI5MGY4MTVmNzc1OWY1OTg2MmE0NWZmOTA4ZWRlMDEzOGZmYmVmNDdlMjE4YWI5NCJ9',
    'laravel_session': 'eyJpdiI6ImF4UmE3b1phOEVDd2N3OWJ1aTdrZHc9PSIsInZhbHVlIjoiUGlUQ1E4NG9PR3hzeGZrcXR1bmN5amlvejl5U2hvUklWNXhHZHJZdEVHVVdkbTFFdjB6d0Vvc1FyWXZGRWRFK1hkQ0pGQlo5R1A1QTZ0K255NVY2dnc9PSIsIm1hYyI6ImZjYzczY2JjZDA0Y2Y3MzQwYTg0YTY3Mzk3ZGUyMzU5OTJjZTNiN2U2MWFiNjlmOTMxZjRmMzA2NzVlMzQwYzMifQ%3D%3D',
}

headers = {
    'Connection': 'keep-alive',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
    'Accept': '*/*',
    'X-Requested-With': 'XMLHttpRequest',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36',
    'Referer': 'http://zipcode.mn/map',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'en',
}

#turluud
categoryMap = {1: 'Улс', 2: 'Хот', 4: 'Аймаг', 5: 'Дүүрэг', 6: 'сум', 8: 'баг', 9: 'Хороо'}

#api duudah
def callData(url):
    response = requests.get(url, headers=headers, cookies=cookies, verify=False, timeout=10)
    return json.loads(response.text)

#coordinate hadgalah
def saveCoordinate(coordinates):
    for coordinate in coordinates:
        id = coordinate['id']
        zipcode = coordinate['zipcode']
        x = coordinate['x']
        y = coordinate['y']
        num = coordinate['num']

        insertData = [
            (id, zipcode, x, y, num)
        ]

        conn.executemany("""
        INSERT INTO zip_coordinate
        (id, zipcode, x, y, num)
        VALUES(?, ?, ?, ?, ?);
        """, insertData)

#relation hadgalah
def saveRelation(relations):
    relations = relations[0]

    id = relations['id']
    relation_code = relations['relation_code']
    zipcode = relations['zipcode']
    beg_point = relations['beg_point']
    end_point = relations['end_point']

    insertData = [
        (id, relation_code, zipcode, beg_point, end_point)
    ]

    conn.executemany("""
    INSERT INTO zip_relation
    (id, relation_code, zipcode, beg_point, end_point)
    VALUES(?, ?, ?, ?, ?);

    """, insertData)

    saveCoordinate(relations['coordinates'])

#location hadgalah
def saveData(data):
    id = data['id']
    parent_id = data['parent_id']
    location_name_mn = data['location_name_mn']
    location_name_en = data['location_name_en']
    zipcode = data['zipcode']
    x = data['x']
    y = data['y']
    category_type_code = data['category_type_code']
    category_type_name = ''

    try: 
        category_type_name = categoryMap[category_type_code]
    except KeyError as e:
        print('not exis category type:', category_type_code)
        

    saveRelation(data['relations'])

    insertData = [
        (id, parent_id, location_name_mn, location_name_en, zipcode, x, y, category_type_code, category_type_name)
    ]
    conn.executemany("""
    INSERT INTO zip_location
    (id, parent_id, location_name_mn, location_name_en, zipcode, x, y, category_type_code, category_type_name)
    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, insertData)

#recursive ajilladag function
def callRec(parentId):
    url = 'http://zipcode.mn/map/getpolydatawithchildren/' + str(parentId)
    data = callData(url)
    parentData = data['data'][0]

    saveData(parentData)

    tmpData = data['children']

    if not tmpData:
        i = 1
    else: 
        for sData in tmpData:
            childId = sData['id']

            print(sData['location_name_mn'])
            
            conn.commit()
            time.sleep(sleepTime)

            callRec(childId)

# aimag hot
aimagData = callData('http://zipcode.mn/map/getanposition')

for data in aimagData:
    aimagId = data['id']
    #1 = mongol ulsaas ylgaatai
    if aimagId != 1:
        callRec(aimagId)

conn.close()
