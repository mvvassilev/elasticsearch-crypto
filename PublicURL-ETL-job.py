import hashlib
import json
import re
from datetime import datetime
import pandas as pd
import requests
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
import sys
from awsglue.utils import getResolvedOptions

# Constants
# data_src_url = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"
# ES_IP = 'http://3.89.129.252/:9200'
# ES_index = "storm_events_detail"
# if cryto_type exists, data_src_url = "https://www.cryptodatadownload.com/cdd/"
# Fetch enviroment variables
# json_format = 1 : for rest_api
# Type of URL : csv files, customized csv file, json_file
def get_glue_args(mandatory_fields, default_optional_args):
    # The glue args are available in sys.argv with an extra '--'
    given_optional_fields_key = list(set([i[2:] for i in sys.argv]).intersection([i for i in default_optional_args]))
    args = getResolvedOptions(sys.argv,
                            mandatory_fields+given_optional_fields_key)
    # Overwrite default value if optional args are provided
    default_optional_args.update(args)
    return default_optional_args

mandatory_fields = ['JOB_NAME','data_src_url', 'ES_IP', 'ES_index']
default_optional_args = {'crypto_type':'', 'json_format':0}

args = get_glue_args(mandatory_fields, default_optional_args)
data_src_url = args['data_src_url']
ES_IP = args['ES_IP']
ES_index = args['ES_index']
crypto_type = args['crypto_type']
json_format = args['json_format']

# Create Spark session
sc = SparkContext()
spark = SparkSession.builder \
    .appName('flow-spark') \
    .getOrCreate()

# Elasticsearch instance
es = Elasticsearch(ES_IP)

def defaultConverter(o):
    if isinstance(o, datetime):
        return o.__str__()

def grantID(doc):
    doc["@timestamp"] = datetime.now()
    _json = json.dumps(doc, default=defaultConverter)
    if "id" not in doc.keys():
        doc['id'] = hashlib.sha224(_json.encode('utf-8')).hexdigest()

    return doc


# Fetch data from the url
if crypto_type:
    # filename: for example,  Binance_BTCGBP_d.csv
    urls = [data_src_url + "_".join(["Binance", crypto_type.replace("/", ""), "d.csv"])]
elif json_format:
    # file is of Json format
    urls = [data_src_url]
else:
    response = requests.get(data_src_url).text
    urls = [data_src_url+item for item in re.findall(r'href=[\'"]?([^\'" >]+)', response) if "StormEvents_detail" in item]


# Check if index exists and delete index data before insertion
if es.indices.exists(index=ES_index):
  es.indices.delete(index=ES_index)

# Create the index with increased max_result_window setting
es.indices.create(index=ES_index, body= {"settings" : { "index.max_result_window" : 500000 }})
def generate_actions():
  for file in urls:
      if json_format: # json_format
        pd_df = pd.read_json(file)
        result = pd_df.to_json(orient="split")
        parsed = json.loads(result)
        columns = parsed["columns"]
        df_type_list = []

        for key in columns:
            df_type_list.append(StructField(key, StringType(), True))

        df_schema = StructType(df_type_list)

        df = spark.createDataFrame(parsed["data"],schema=df_schema)

        data = df.toJSON().map(lambda j: json.loads(j)).map(grantID).collect()
      else: # csv_format
        if crypto_type: # crypto
            pd_df = pd.read_csv(file, header=None, skiprows=1)
        else: # bulk csv file
            pd_df = pd.read_csv(file, header=None)
        first_row = pd_df.iloc[0].tolist()
        df_type_list = []

        for key in first_row:
            df_type_list.append(StructField(key, StringType(), True))

        df_schema = StructType(df_type_list)

        pd_df = pd_df.iloc[1:]
        df = spark.createDataFrame(pd_df,schema=df_schema)
            
        data = df.toJSON().map(lambda j: json.loads(j)).map(grantID).collect()

      for i in range(1, len(data)):
        doc = data[i]
        doc["_id"] = doc['id']
        yield doc

cnt_creation = 0
for ok, action in streaming_bulk(
    client=es, index=ES_index, actions=generate_actions(),
):
  cnt_creation += ok
  print("Total creation: %d" % cnt_creation)