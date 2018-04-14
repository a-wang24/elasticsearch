# -*- coding: utf-8 -*-
"""
Created on Sun Oct 15 00:14:14 2017

@author: Alan
"""

import json

i = 0
new_bus_content = open('C:\\Users\\Alan\\AnacondaProjects\\elasticsearch\\yelp_data\\new_business.json', 'w')
with open('C:\\Users\\Alan\\AnacondaProjects\\elasticsearch\\yelp_data\\business.json') as business_content:
     for line in business_content:
         print("line", i)
         i = i+1
         json_line = json.loads(line)
         if json_line['latitude'] == None:
            json_line['location'] = None
            json_line.pop('latitude', None)
            json_line.pop('longitude', None)
         else:
            json_line['location'] = {"lat": json_line['latitude'], "lon": json_line['longitude']}
         for key, value in json_line['hours'].items():
             json_line['hours'][key] = {'open': value.split('-')[0], 'close': value.split('-')[1]}
         ind_line = {"_index": "yelp_businesses", "_type": "business", "_id": json_line["business_id"]}
         json.dump(ind_line,new_bus_content)
         new_bus_content.write('\n')
         json.dump(json_line, new_bus_content)
         new_bus_content.write('\n')