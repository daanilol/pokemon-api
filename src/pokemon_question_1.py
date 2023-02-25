# Databricks notebook source
import requests
import json
import pyspark
from pyspark.sql.functions import udf, col, max as _max
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, MapType, BooleanType

# COMMAND ----------

url = 'https://pokeapi.co/api/v2/pokemon/?limit=-1'
archive = json.loads(requests.get(url).text)['results']

# COMMAND ----------

df = spark.createDataFrame(archive)

# COMMAND ----------

sub_schema_move = MapType(StringType(), StringType())

sub_schema_version_group_details = ArrayType(StructType(
    [
        StructField('level_learned_at', IntegerType()),
        StructField('move_learn_method', MapType(StringType(), StringType())),
        StructField('version_group', MapType(StringType(), StringType()))        
    ]
))

moves_schema = ArrayType(StructType(
    [
        StructField('move', sub_schema_move),
        StructField('version_group_details', sub_schema_version_group_details)
    ]
))

stats_schema = ArrayType(StructType(
    [
        StructField('base_stat', IntegerType()),
        StructField('effort', IntegerType()),
        StructField('stat', MapType(StringType(), StringType()))
    ]
))

types_schema = ArrayType(StructType(
    [
        StructField('type', MapType(StringType(), StringType()))
    ]
))

default_schema = StructType(
        [
            StructField('abilities', StringType()),
            StructField('base_experience', IntegerType()),
            StructField('game_indices', StringType()),
            StructField('height', IntegerType()),
            StructField('held_items', StringType()),
            StructField('id', IntegerType()),
            StructField('is_default', BooleanType()),
            StructField('location_area_encounters', StringType()),
            StructField('moves', moves_schema),
            StructField('name', StringType()),
            StructField('order', IntegerType()),
            StructField('past_types', StringType()),
            StructField('species', MapType(StringType(), StringType())),
            StructField('sprites', StringType()),
            StructField('stats', stats_schema),
            StructField('types', types_schema),
            StructField('weight', IntegerType()),
        ]
)


@udf(returnType=default_schema)
def get_data_default(x):
    return json.loads(requests.get(x).text)


df = df.withColumn('pokemon_json', get_data_default(col('url'))).select('name', 'url', 'pokemon_json')

# COMMAND ----------

df = df.withColumn('poke_height', df.pokemon_json.getItem('height'))

# COMMAND ----------

height_value = df.select(_max('poke_height')).collect()[0][0]

# COMMAND ----------

df.where(df.poke_height == height_value).select('name', 'poke_height').display()