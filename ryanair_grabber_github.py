#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 26 21:20:36 2018

"""

import urllib.request
import datetime
import json
import mysql.connector
from time import sleep
import random
import time

german_letters = {'ü':'u', 'ö':'o', 'ä':'a', 'Ü':'U', 'Ö':'O', 'Ä':'A'}

print ('Started, first wait.')

sleep (3600*4)

today_raw = datetime.datetime.today()

print ('Started at: %s.' % today_raw.strftime('%Y-%m-%d %H:%M:%S'))


cnx = mysql.connector.connect(user='', password='',
                              host='',
                              database='')

cur = cnx.cursor()


#from_airports = ['BGY']
from_airports = ['FRA', 'HHN', 'BGY', 'CIA', 'VLC', 'STR']

start1 = time.time()

sql_prepare = 'CALL prepare_for_prices();'
cur.execute (sql_prepare)
cnx.commit()

for airport in from_airports:
    start2 = time.time()    
    for i in range(1,150):
        on_date = (today_raw + datetime.timedelta(days = i)).strftime('%Y-%m-%d')
        
        
        sget_url = 'https://api.ryanair.com/farefinder/3/oneWayFares?departureAirportIataCode=%s&language=en&outboundDepartureDateFrom=%s&outboundDepartureDateTo=%s&priceValueTo=500' % (airport, on_date, on_date)
        
        with urllib.request.urlopen(sget_url) as responce:
            result = responce.read()
            
        json_result = json.loads(result)['fares']
        
        
        
        for travels in json_result:
            sql_insert_departure = "CALL add_airport('%s', '%s', '%s')" % (travels['outbound']['departureAirport']['iataCode'], travels['outbound']['departureAirport']['name'], travels['outbound']['departureAirport']['countryName'])
             
            for k, v in german_letters.items():
                sql_insert_departure = sql_insert_departure.replace(k,v)
            
            cur.execute (sql_insert_departure)
            
            sql_insert_arrival = "CALL add_airport('%s', '%s', '%s')" % (travels['outbound']['arrivalAirport']['iataCode'], travels['outbound']['arrivalAirport']['name'], travels['outbound']['arrivalAirport']['countryName'])

            for k, v in german_letters.items():
                sql_insert_arrival = sql_insert_arrival.replace(k,v)

            cur.execute (sql_insert_arrival)
            
            sql_insert_add = "CALL add_update('%s', '%s', '%s', '%s', '%s', '%s')" % (travels['outbound']['departureAirport']['iataCode'],travels['outbound']['arrivalAirport']['iataCode'], travels['outbound']['departureDate'].replace('T', ' '), travels['outbound']['arrivalDate'].replace('T', ' '), travels['outbound']['price']['value'],today_raw.strftime('%Y-%m-%d %H:%M:%S'))    
            cur.execute (sql_insert_add)
            
        cnx.commit()
        
        dd = 1 + random.lognormvariate(0,1)*3
        #print('Delay: %.2f seconds' % dd)
        sleep (dd)
        
    end = time.time()  
    print ('%s: Airport %s is done. Time elapsed for it: %.2f seconds. Time elapsed total %.2f seconds)' % (datetime.datetime.today(), airport, end-start2, end-start1))

sql_finished = 'CALL update_current_prices();'
cur.execute (sql_finished)  
cnx.commit()  
    
cur.close()
cnx.close()
