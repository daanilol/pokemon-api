# Databricks notebook source
import requests
import json
import pyspark
from pyspark.sql.functions import udf, col, max as _max
from pyspark.sql.types import StructType, StructField, StringType, MapType, IntegerType, BooleanType, ArrayType

# COMMAND ----------

url = 'https://pokeapi.co/api/v2/type/15/'
archive = json.loads(requests.get(url).text)['pokemon']

# COMMAND ----------

pokemon_schema = StructType([
    StructField('pokemon', MapType(StringType(), StringType()))
])

df_ice_pokemon = spark.createDataFrame(archive, pokemon_schema)

# COMMAND ----------

schema = StructType(
    [
        StructField('abilities', StringType()),
        StructField('base_experience', IntegerType()),
        StructField('forms', StringType()),
        StructField('game_indices', StringType()),
        StructField('height', StringType()),
        StructField('held_items', StringType()),
        StructField('id', StringType()),
        StructField('is_default', BooleanType()),
        StructField('location_area_encounters', StringType()),
        StructField('moves', StringType()),
        StructField('name', StringType()),
        StructField('order', StringType()),
        StructField('past_types', StringType()),
        StructField('species', StringType()),
        StructField('sprites', StringType()),
        StructField('stats', StringType()),
        StructField('types', StringType()),
        StructField('weight', StringType())
    ]
)

@udf(returnType=schema)
def get_json(x):
    return json.loads(requests.get(x['url']).text)

# COMMAND ----------

df_ice_pokemon = df_ice_pokemon.withColumn('ice_pokemon', get_json(col('pokemon'))).select('ice_pokemon')
df_ice_pokemon = df_ice_pokemon.withColumn('pokemon', df_ice_pokemon.ice_pokemon.getItem('name'))
df_ice_pokemon = df_ice_pokemon.withColumn('is_default', df_ice_pokemon.ice_pokemon.getItem('is_default'))
df_ice_pokemon = df_ice_pokemon.withColumn('base_experience', df_ice_pokemon.ice_pokemon.getItem('base_experience'))
df_ice_pokemon = df_ice_pokemon.where(df_ice_pokemon.is_default == True)
df_ice_pokemon = df_ice_pokemon.select('pokemon', 'base_experience')

# COMMAND ----------

max_base_experience = df_ice_pokemon.select(_max(col('base_experience'))).collect()[0][0]

# COMMAND ----------

df_ice_pokemon.where(col('base_experience') == max_base_experience).display()