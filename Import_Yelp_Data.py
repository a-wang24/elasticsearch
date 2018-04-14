# -*- coding: utf-8 -*-
"""
Created on Fri Sep 29 21:35:48 2017

@author: Alan
Import yelp data into elasticsearch
"""
from datetime import datetime
from elasticsearch import Elasticsearch
#from elasticsearch import GeoPoint
#from elasticsearch_dsl import Index
import json

# default connection to localhost:9200
# Using x-pack include user login
es = Elasticsearch(['http://elastic:changeme@localhost:9200'])

# =============================================================================
# Create business index
request_body = {
        "settings" : {
                "number_of_shards": 5,
                "number_of_replicas": 1
        },
        'mappings': {
            'review':{
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
            },
            'business': {
                '_parent': { "type": "review"},
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
            'user':{
                '_parent': { "type": "review"},
                'properties': {
                        'user_id': {'type': 'keyword'},
                        'name': {'type': 'keyword'},
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
       }
    }
}
                        
print("Creating 'business' index...")
es.indices.create(index = 'yelp', body = request_body, request_timeout=60)

i = 0
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
         es.index(index='yelp_data', doc_type='business', id=json_line['business_id'], body=json_line, request_timeout=30)
         
#i = 100877
#with open('C:\\Users\\Alan\\AnacondaProjects\\elasticsearch\\yelp_data\\business.json') as business_content:
#    for j in range(100876):
#        business_content.__next__()
#    for line in business_content:
#        print("line", i)
#        i=i+1
#        json_line = json.loads(line)
#        if json_line['latitude'] == None:
#            json_line['location'] = None
#            json_line.pop('latitude', None)
#            json_line.pop('longitude', None)
#        else:
#            json_line['location'] = {"lat": json_line['latitude'], "lon": json_line['longitude']}
#        for key, value in json_line['hours'].items():
#            json_line['hours'][key] = {'open': value.split('-')[0], 'close': value.split('-')[1]}
#        es.index(index='yelp', doc_type='business', id=json_line['business_id'], body=json_line, request_timeout=30)
# =============================================================================
    
# =============================================================================
# Create review index
request_body = {
"settings" : {
        "number_of_shards": 5,
        "number_of_replicas": 1
},
'mappings': {
    'review':{
        'properties': {
            'review_id': {'index': 'true', 'type': 'keyword'},
            'user_id': {'index': 'true', 'type': 'keyword'},
            'business_id': {'index': 'true', 'type': 'keyword'},
            'stars': {'type': 'integer'},
            'date': {'type': 'date', 'format': 'YYYY-MM-DD'},
            'text': {'type': 'text', 'fields': { 'raw': {'type': 'keyword'}, 'english': {'type': 'text', 'analyzer': 'english'}}},
            'useful': {'type': 'integer'},
            'funny': {'type': 'integer'},
            'cool': {'type': 'integer'}
            }
       }
    }
}
                        
print("Creating 'reviews' index...")
es.indices.create(index = 'yelp_reviews', body = request_body)
i = 0
with open('C:\\Users\\Alan\\AnacondaProjects\\elasticsearch\\yelp_data\\user.json') as review_content:
     for line in review_content:
         print("line:", i)
         i = i+1
         json_line = json.loads(line)
         es.index(index='reviews', doc_type='review', id=json_line['review_id'], body=json_line)
# =============================================================================
         
# =============================================================================
# Create user index
request_body = {
"settings" : {
        "number_of_shards": 5,
        "number_of_replicas": 1
},
'mappings': {
    'review':{
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
       }
    }
}
                        
print("Creating 'user' index...")
es.indices.create(index = 'user', body = request_body)
i = 0
with open('C:\\Users\\Alan\\AnacondaProjects\\elasticsearch\\yelp_data\\review.json', encoding="utf8") as user_content:
    for line in user_content:
        print("line:", i)
        i = i+1
        json_line = json.loads(line)
        es.index(index='user', doc_type='user', id=json_line['user_id'], body=json_line)
# =============================================================================
         
# =============================================================================
# Create checkin index
request_body = {
"settings" : {
        "number_of_shards": 5,
        "number_of_replicas": 1
},
'mappings': {
            'checkin': {
                    'properties': {
                            'business_id': {'index': 'true', 'type': 'keyword'},
                            'time': {
                                    'properties': {
                                            'Monday': {
                                                    'properties': {
                                                            '14:00': {'type': 'integer'}
                                                            }
                                                    },
                                            'Tuesday': {
                                                    'properties': {
                                                            '14:00': {'type': 'integer'}
                                                            }
                                                    },
                                            'Wednesday': {
                                                    'properties': {
                                                            '14:00': {'type': 'integer'}
                                                            }
                                                    },
                                            'Friday': {
                                                    'properties': {
                                                            '14:00': {'type': 'integer'}
                                                            }
                                                    },
                                            'Thursday': {
                                                    'properties': {
                                                            '14:00': {'type': 'integer'}
                                                            }
                                                    },
                                            'Saturday': {
                                                    'properties': {
                                                            '14:00': {'type': 'integer'}
                                                            }
                                                    },
                                            'Sunday': {
                                                    'properties': {
                                                            '14:00': {'type': 'integer'}
                                                            }
                                                    }
                                            }
                                    }
                    }
                }
    }
}
                        
print("Creating 'checkin' index...")
es.indices.create(index = 'checkin', body = request_body)
i = 0
with open('C:\\Users\\Alan\\AnacondaProjects\\elasticsearch\\yelp_data\\checkin.json', encoding="utf8") as checkin_content:
     for line in checkin_content:
         print("line:", i)
         i = i+1
         json_line = json.loads(line)
         es.index(index='checkin', doc_type='checkin', body=json_line)
# =============================================================================    
         
# =============================================================================
# Create tip index
request_body = {
"settings" : {
        "number_of_shards": 5,
        "number_of_replicas": 1
},
'mappings': {
            'tip': {
                    'properties': {
                            'text': {'type': 'text'},
                            'date': {'type': 'date', 'format': 'YYYY-MM-DD'},
                            'likes': {'type': 'integer'},
                            'business_id': {'type': 'keyword'},
                            'user_id': {'type': 'keyword'}
                    }
                }
            }
}
print("Creating 'tip' index...")
es.indices.create(index = 'tip', body = request_body)
i = 0
with open('C:\\Users\\Alan\\AnacondaProjects\\elasticsearch\\yelp_data\\tip.json', encoding="utf8") as tip_content:
     for line in tip_content:
         print("line:", i)
         i = i+1
         json_line = json.loads(line)
         es.index(index='tip', doc_type='tip', body=json_line)
# =============================================================================    
         
# =============================================================================
# Create photo index
request_body = {
"settings" : {
        "number_of_shards": 5,
        "number_of_replicas": 1
},         
'mappings': {
          'photo': {
                    'properties': {
                            'photo_id': {'type': 'keyword'},
                            'business_id': {'type': 'keyword'},
                            'caption': {'type': 'text'},
                            'label': {'type': 'keyword'}
                    }
                }
        }
}
print("Creating 'photo' index...")
es.indices.create(index = 'photo', body = request_body)
i = 0
with open('C:\\Users\\Alan\\AnacondaProjects\\elasticsearch\\yelp_data\\photos.json', encoding="utf8") as photo_content:
     for line in photo_content:
         print("line:", i)
         i = i+1
         json_line = json.loads(line)
         es.index(index='photo', doc_type='photo', id=json_line['photo_id'], body=json_line)
         #            'review': {
#                    'properties': {
#                            'review_id': {'index': 'true', 'type': 'keyword'},
#                            'user_id': {'index': 'true', 'type': 'keyword'},
#                            'business_id': {'index': 'true', 'type': 'keyword'},
#                            'stars': {'type': 'float'},
#                            'date': {'type': 'date', 'format': 'YYYY-MM-DD'},
#                            'text': {'type': 'text'},
#                            'useful': {'type': 'integer'},
#                            'funny': {'type': 'integer'},
#                            'cool': {'type': 'integer'}
#                    }
#                },
#            'user':{
#                    'properties': {
#                            'user_id': {'index': 'true', 'type': 'keyword'},
#                            'name': {'index': 'true', 'type': 'keyword'},
#                            'review_count': {'type': 'integer'},
#                            'yelping_since': {'type': 'date', 'format': 'YYYY-MM-DD'},
#                            'friends': {'type': 'text'},
#                            'useful': {'type': 'integer'},
#                            'funny': {'type': 'integer'},
#                            'cool': {'type': 'integer'},
#                            'fans': {'type': 'integer'},
#                            'elite': {'type': 'integer'},
#                            'average_stars': {'type': 'float'},
#                            'compliment_hot': {'type': 'integer'},
#                            'compliment_more': {'type': 'integer'},
#                            'compliment_profile': {'type': 'integer'},
#                            'compliment_cute': {'type': 'integer'},
#                            'compliment_list': {'type': 'integer'},
#                            'compliment_note': {'type': 'integer'},
#                            'compliment_plain': {'type': 'integer'},
#                            'compliment_cool': {'type': 'integer'},
#                            'compliment_funny': {'type': 'integer'},
#                            'compliment_writer': {'type': 'integer'},
#                            'compliment_photos': {'type': 'integer'}
#                    }
#                },
#            'checkin': {
#                    'properties': {
#                            'business_id': {'index': 'true', 'type': 'keyword'},
#                            'time': {
#                                    'properties': {
#                                            'Monday': {
#                                                    'properties': {
#                                                            '14:00': {'type': 'integer'}
#                                                            }
#                                                    },
#                                            'Tuesday': {
#                                                    'properties': {
#                                                            '14:00': {'type': 'integer'}
#                                                            }
#                                                    },
#                                            'Wednesday': {
#                                                    'properties': {
#                                                            '14:00': {'type': 'integer'}
#                                                            }
#                                                    },
#                                            'Friday': {
#                                                    'properties': {
#                                                            '14:00': {'type': 'integer'}
#                                                            }
#                                                    },
#                                            'Thursday': {
#                                                    'properties': {
#                                                            '14:00': {'type': 'integer'}
#                                                            }
#                                                    },
#                                            'Saturday': {
#                                                    'properties': {
#                                                            '14:00': {'type': 'integer'}
#                                                            }
#                                                    },
#                                            'Sunday': {
#                                                    'properties': {
#                                                            '14:00': {'type': 'integer'}
#                                                            }
#                                                    }
#                                            }
#                                    }
#                    }
#                },
#            'tip': {
#                    'properties': {
#                            'text': {'type': 'text'},
#                            'date': {'type': 'date', 'format': 'YYYY-MM-DD'},
#                            'likes': {'type': 'integer'},
#                            'business_id': {'type': 'keyword'},
#                            'user_id': {'type': 'keyword'}
#                    }
#                },
#            'photo': {
#                    'properties': {
#                            'photo_id': {'type': 'keyword'},
#                            'business_id': {'type': 'keyword'},
#                            'caption': {'type': 'text'},
#                            'label': {'type': 'keyword'}
#                    }
#                }