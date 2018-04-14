# -*- coding: utf-8 -*-
"""
Created on Sat Oct 14 23:34:00 2017

@author: Alan
"""

from elasticsearch import Elasticsearch, helpers
import json

def fix_business_json(line):
    json_line = json.loads(line)
    if json_line['latitude'] == None:
       json_line['location'] = None
       json_line.pop('latitude', None)
       json_line.pop('longitude', None)
    else:
       json_line['location'] = {"lat": json_line['latitude'], "lon": json_line['longitude']}
    for key, value in json_line['hours'].items():
        json_line['hours'][key] = {'open': value.split('-')[0], 'close': value.split('-')[1]}
    return json_line
            
def fix_review_json(line):
    json_line = json.loads(line)
    json_line['review_stars'] = json_line.pop('stars')
    return json_line

def fix_user_json(line):
    json_line = json.loads(line)
    return json_line

es = Elasticsearch(['http://elastic:changeme@localhost:9200'])

request_body = {
        "settings" : {
                "number_of_shards": 5,
                "number_of_replicas": 1
        },'mappings': {
                 'business': {
                'properties': {
                    'business_id': {'type': 'keyword'}, #update
                    'name': {'type': 'keyword'},
                    'neighborhood': {'index': 'analyzed', 'type': 'text'}, #update
                    'address': {'type': 'keyword'},
                    'city': {'type': 'keyword'},
                    'state': {'type': 'keyword'},
                    'postal code': {'type': 'keyword'},
                    'location': {'type': 'geo_point'},
                    'stars': {'type': 'float'},
                    'review_count': {'type': 'integer'},
                    'is_open': {'type': 'integer'},
                    'attributes': {'type': 'nested'},
                    'categories': {'type': 'text', 'fields': {'raw': {'type': 'keyword'}, 'english': {'type':'text', 'analyzer': 'english'}}}, #update
                    'hours': {
                        'type': 'nested',
                        'properties': {
                            'Monday': {
                                    'properties':{
                                            'open': {'type': 'date', 'format': 'H:mm'},
                                            'close': {'type': 'date', 'format': 'H:mm'}}},
                            'Tuesday': {
                                    'properties':{
                                            'open': {'type': 'date', 'format': 'H:mm'},
                                            'close': {'type': 'date', 'format': 'H:mm'}}},
                            'Wednesday': {
                                    'properties':{
                                            'open': {'type': 'date', 'format': 'H:mm'},
                                            'close': {'type': 'date', 'format': 'H:mm'}}},
                            'Thursday': {
                                    'properties':{
                                            'open': {'type': 'date', 'format': 'H:mm'},
                                            'close': {'type': 'date', 'format': 'H:mm'}}},
                            'Friday': {
                                    'properties':{
                                            'open': {'type': 'date', 'format': 'H:mm'},
                                            'close': {'type': 'date', 'format': 'H:mm'}}},
                            'Saturday': {
                                        'properties':{
                                            'open': {'type': 'date', 'format': 'H:mm'},
                                            'close': {'type': 'date', 'format': 'H:mm'}}},
                            'Sunday': {
                                    'properties':{
                                            'open': {'type': 'date', 'format': 'H:mm'},
                                            'close': {'type': 'date', 'format': 'H:mm'}}}
                           }
                        }
                    }
                },
                            'review':{
                                    '_parent': {'type': "business"},
                'properties': {
                    'review_id': {'index': 'true', 'type': 'keyword'},
                    'user_id': {'index': 'true', 'type': 'keyword'},
                    'business_id': {'index': 'true', 'type': 'keyword'},
                    'review_stars': {'type': 'integer'},
                    'date': {'type': 'date', 'format': 'YYYY-MM-DD'},
                    'text': {'type': 'text', 'fields': { 'raw': {'type': 'keyword'}, 'english': {'type': 'text', 'analyzer': 'english', 'fielddata': 'true'}}},
                    'useful': {'type': 'integer'},
                    'funny': {'type': 'integer'},
                    'cool': {'type': 'integer'}
                }
            }
        }
}

request_body = {
"settings" : {
        "number_of_shards": 5,
        "number_of_replicas": 1
},
'mappings': {
    'user':{
        'properties': {
            'user_id': {'index': 'true', 'type': 'keyword'},
            'name': {'index': 'true', 'type': 'keyword'},
            'review_count': {'type': 'integer'},
            'yelping_since': {'type': 'date', 'format': 'YYYY-MM-DD'},
            'friends': {'type': 'keyword'},
            'useful': {'type': 'integer'},
            'funny': {'type': 'integer'},
            'cool': {'type': 'integer'},
            'fans': {'type': 'integer'},
            'elite': {'type': 'integer'},
            'average_stars': {'type': 'float'},
            'compliment_hot': {'type': 'integer'},
            'compliment_more': {'type': 'integer'},
            'compliment_profile': {'type': 'integer'},
            'compliment_cute': {'type': 'integer'},
            'compliment_list': {'type': 'integer'},
            'compliment_note': {'type': 'integer'},
            'compliment_plain': {'type': 'integer'},
            'compliment_cool': {'type': 'integer'},
            'compliment_funny': {'type': 'integer'},
            'compliment_writer': {'type': 'integer'},
            'compliment_photos': {'type': 'integer'}
            }
       },
                                    'review':{
                                    '_parent': {'type': "business"},
                'properties': {
                    'review_id': {'index': 'true', 'type': 'keyword'},
                    'user_id': {'index': 'true', 'type': 'keyword'},
                    'business_id': {'index': 'true', 'type': 'keyword'},
                    'review_stars': {'type': 'integer'},
                    'date': {'type': 'date', 'format': 'YYYY-MM-DD'},
                    'text': {'type': 'text', 'fields': { 'raw': {'type': 'keyword'}, 'english': {'type': 'text', 'analyzer': 'english'}}},
                    'useful': {'type': 'integer'},
                    'funny': {'type': 'integer'},
                    'cool': {'type': 'integer'}
                }
            }
    }
}
        
es.indices.create(index = 'yelp_business', body = request_body, request_timeout=60)
es.indices.create(index = 'yelp_users', body = request_body, request_timeout=60)

business_content =  open('C:\\Users\\Alan\\AnacondaProjects\\elasticsearch\\yelp_data\\business.json', encoding='utf8')
actions = [
        {
            '_index': 'yelp_business',
            '_type': 'business',
            '_id': fix_business_json(line)['business_id'],
            '_source': fix_business_json(line)
        }
        for line in business_content
]

review_content = open('C:\\Users\\Alan\\AnacondaProjects\\elasticsearch\\yelp_data\\review.json', encoding='utf8')
actions = [
        {
            '_index': 'yelp_business',
            '_type': 'review',
            '_id': fix_review_json(line)['review_id'],
            '_source': fix_review_json(line),
            '_parent': fix_review_json(line)['business_id']
        }
        for line in review_content
]

user_content = open('C:\\Users\\Alan\\AnacondaProjects\\elasticsearch\\yelp_data\\user.json', encoding='utf8')