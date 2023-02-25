# Databricks notebook source
import requests
import json
import pyspark
from pyspark.sql.functions import col, udf, explode
from pyspark.sql.types import StructType, StructField, MapType, StringType, IntegerType, ArrayType, BooleanType, NullType

# COMMAND ----------

stats_schema = ArrayType(StructType(
    [
        StructField('base_stat', IntegerType()),
        StructField('effort', StringType()),
        StructField('stat', MapType(StringType(), StringType()))
    ]))

schema = StructType(
    [
        StructField('abilities', StringType()),
        StructField('base_experience', StringType()),
        StructField('forms', StringType()),
        StructField('game_indices', StringType()),
        StructField('height', StringType()),
        StructField('held_items', StringType()),
        StructField('id', StringType()),
        StructField('is_default', StringType()),
        StructField('location_area_encounters', StringType()),
        StructField('moves', StringType()),
        StructField('name', StringType()),
        StructField('order', StringType()),
        StructField('past_types', StringType()),
        StructField('species', StringType()),
        StructField('sprites', StringType()),
        StructField('stats', stats_schema),
        StructField('types', StringType()),
        StructField('weight', StringType())        
    ])

def get_default_pokemon_json():
    url = 'https://pokeapi.co/api/v2/pokemon/?limit=-1'
    return json.loads(requests.get(url).text)['results']


@udf(returnType=schema)
def get_pokemon_json(x):
    return json.loads(requests.get(x).text)

df_pokemon = spark.createDataFrame(get_default_pokemon_json())
df_pokemon = df_pokemon.withColumn('pokemon_json', get_pokemon_json(col('url')))

# COMMAND ----------

def get_chain_evolution_json_url():
    url = 'https://pokeapi.co/api/v2/evolution-chain/?limit=-1'
    return json.loads(requests.get(url).text)['results']

evolves_to_2 = ArrayType(StructType(
    [
      StructField('evolution_details', NullType()),
      StructField('evolves_to', StringType()),
      StructField('is_baby', BooleanType()),
      StructField('species', MapType(StringType(), StringType()))
    ]))

evolves_to_1 = ArrayType(StructType(
    [
      StructField('evolution_details', NullType()),
      StructField('evolves_to', evolves_to_2),
      StructField('is_baby', BooleanType()),
      StructField('species', MapType(StringType(), StringType()))
    ]))

evolves_to_0 = ArrayType(StructType(
    [
              StructField('evolution_details', NullType()),
      StructField('evolves_to', evolves_to_1),
      StructField('is_baby', BooleanType()),
      StructField('species', MapType(StringType(), StringType()))
    ]))

chain = StructType(
    [
      StructField('evolution_details', NullType()),
      StructField('evolves_to', evolves_to_0),
      StructField('is_baby', BooleanType()),
      StructField('species', MapType(StringType(), StringType()))
    ])

chain_evolution_schema = StructType(
    [
      StructField('baby_trigger_item', NullType()),
      StructField('chain', chain),
      StructField('id', IntegerType())
    ])

@udf(returnType=chain_evolution_schema)
def get_chain_evolution_json(x):
    return json.loads(requests.get(x).text)


@udf(returnType=ArrayType(StringType()))
def get_pokemon_and_evolutions(x):
    default_list = list()
    first_evo = list()
    second_evo = list()

    default_list.append(x['chain']['species']['name'])
    for evo_0 in x['chain']['evolves_to']:
        first_evo.append(evo_0['species']['name'])
        for evo_1 in evo_0['evolves_to']:
            second_evo.append(evo_1['species']['name'])

    default_list.append(first_evo)
    default_list.append(second_evo)
    return default_list

# COMMAND ----------

df_chain = spark.createDataFrame(get_chain_evolution_json_url())
df_chain.withColumn('json', get_chain_evolution_json(col('url'))).display()

# COMMAND ----------

