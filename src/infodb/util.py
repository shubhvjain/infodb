import json
from jsonschema import validate
import time
from uuid import uuid4
import datetime

def find_keys_missing(keys_to_check,data):
  missing = []
  for key in keys_to_check:
    if key not in data:
      missing.append(key)
  return missing

def process_schema(schema_string):
  try:
      valid_schema_doc = {
         "type":"object",
         "properties":{
            "title":{"type":"string"},
            "description":{"type":"string"},
            "type":{"type":"string"},
            "properties":{"type":"object"},
            "primary_key":{"type":"array","items":{"type":"string"},"uniqueItems": True},
            "update_allowed":{"type":"array","items":{"type":"string"},"uniqueItems": True}
         },
         "required":["title","description","properties","primary_key","update_allowed"]
      }
      data = json.loads(schema_string)
      data["type"]="object"
      validate(data,valid_schema_doc)
      
      required_keys = ["title","description","type","properties","primary_key","update_allowed"]
      missing_required_keys = find_keys_missing(required_keys,data)
      if len(missing_required_keys)>0:
         raise Exception("Invalid schema. Following fields missing: "+str(missing_required_keys))
      """
      check if primary key and updated_allowed has a intersection
      all primary keys must be of they string 
      """

      return data
  except Exception as e:
     print("Error")
     raise e

def get_current_unix_timestamp():
  return int(time.time())

def get_current_timestamp_human():
  c_ts = get_current_unix_timestamp()
  return datetime.datetime.fromtimestamp(c_ts).strftime('%Y-%m-%d %H:%M:%S') 

def generate_blank_doc(schema_name,schema):
  c_ts = get_current_unix_timestamp()
  new_doc = {
      '_id': uuid4().hex,
      "data":{},
      "meta":{
         "created_on":c_ts,
         "tags":[],
         "note":"Created on :  " + get_current_timestamp_human()
      },
      "schema":schema_name,
   }
  return new_doc

def are_all_keys_allowed(input_dict, allowed_fields):
    # Check if all keys in the dictionary are in the list of allowed fields
    return all(key in allowed_fields for key in input_dict.keys())

def update_dict(main, update):
    for key, value in update.items():
        main[key] = value
    return main