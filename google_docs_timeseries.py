#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 16 20:34:37 2018

"""
from datetime import date
import gspread
from oauth2client.service_account import ServiceAccountCredentials

import mysql.connector

scope = ['https://spreadsheets.google.com/feeds']

credentials = ServiceAccountCredentials.from_json_keyfile_name('creds.json', scope)

file = gspread.authorize(credentials) # authenticate with Google
sheet = file.open_by_key("").sheet1
#sheet.update_acell('B1', date.today().strftime('%d/%m/%Y'))

cnx = mysql.connector.connect(user='', password='',
                              host='',
                              database='')

cur = cnx.cursor()

sql_request = "select dep_datetime, IATA_FROM, IATA_TO, price, search_date, flight_id from  (select ID, flight_id, search_date, price from prices_now) as p join flights on p.flight_id = flights.id  join routes on flights.ID_route = routes.ID where IATA_FROM = 'FRA' and weekday(dep_datetime)=%i and IATA_TO = 'BGY';" % 3
cur.execute (sql_request)
rows = cur.fetchall()

cell_list = sheet.range('A3:B40')

cells_thursday_date = cell_list[::2]
cells_thursday_price = cell_list[1::2]


i = 3
for row, cell_date, cell_price in zip(rows, cells_thursday_date, cells_thursday_price):
    cell_date.value = row[0].strftime('%d/%m/%Y')
    cell_price.value = row[3]

cell_result = []
cell_result += cell_list



sql_request = "select dep_datetime, IATA_FROM, IATA_TO, price, search_date, flight_id from  (select ID, flight_id, search_date, price from prices_now) as p join flights on p.flight_id = flights.id  join routes on flights.ID_route = routes.ID where IATA_FROM = 'FRA' and weekday(dep_datetime)=%i and IATA_TO = 'BGY';" % 4
cur.execute (sql_request)
rows = cur.fetchall()

cell_list = sheet.range('C3:D40')

cells_thursday_date = cell_list[::2]
cells_thursday_price = cell_list[1::2]

i = 3
for row, cell_date, cell_price in zip(rows, cells_thursday_date, cells_thursday_price):
    cell_date.value = row[0].strftime('%d/%m/%Y')
    cell_price.value = row[3]

cell_result += cell_list

sql_request = "select dep_datetime, IATA_FROM, IATA_TO, price, search_date, flight_id from  (select ID, flight_id, search_date, price from prices_now) as p join flights on p.flight_id = flights.id  join routes on flights.ID_route = routes.ID where IATA_FROM = 'BGY' and weekday(dep_datetime)=%i and IATA_TO = 'FRA';" % 6
cur.execute (sql_request)
rows = cur.fetchall()

cell_list = sheet.range('F3:G40')

cells_thursday_date = cell_list[::2]
cells_thursday_price = cell_list[1::2]

i = 3
for row, cell_date, cell_price in zip(rows, cells_thursday_date, cells_thursday_price):
    cell_date.value = row[0].strftime('%d/%m/%Y')
    cell_price.value = row[3]

cell_result += cell_list
sheet.update_cells(cell_result)

cnx.commit()  
    
cur.close()
cnx.close()
