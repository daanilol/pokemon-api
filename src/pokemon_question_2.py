# Databricks notebook source
import requests
import json
import pyspark
from pyspark.sql.functions import col, udf, size, lit, array, array_contains
from pyspark.sql.types import StructType, StructField, MapType, StringType, IntegerType, ArrayType, BooleanType

# COMMAND ----------

url = 'https://pokeapi.co/api/v2/evolution-chain/?limit=-1'
archive = json.loads(requests.get(url).text)['results']

# COMMAND ----------

schema_0 = MapType(StringType(), StringType())
df_chain_evolution = spark.createDataFrame(archive, schema_0)

# COMMAND ----------

evolves_to_schema_2 = ArrayType(StructType([
    StructField('evolution_details', StringType()),
    StructField('evolves_to', StringType()),
    StructField('is_baby', StringType()),
    StructField('species', MapType(StringType(), StringType()))
]))

evolves_to_schema_1 = ArrayType(StructType([
    StructField('evolution_details', StringType()),
    StructField('evolves_to', evolves_to_schema_2),
    StructField('is_baby', StringType()),
    StructField('species', MapType(StringType(), StringType()))
]))

schema = StructType([
    StructField('evolution_details', StringType()),
    StructField('evolves_to', evolves_to_schema_1),
    StructField('is_baby', StringType()),
    StructField('species', MapType(StringType(), StringType()))
])

@udf(returnType=schema)
def get_chain_evolution(x):
    return json.loads(requests.get(x['url']).text)['chain']

# COMMAND ----------

df_chain_evolution = df_chain_evolution.withColumn('chain_evolution', get_chain_evolution(col('value')))
df_chain_evolution = df_chain_evolution.withColumn('pokemon', df_chain_evolution.chain_evolution.getItem('species').getItem('name')).select('pokemon', 'chain_evolution')
df_chain_evolution = df_chain_evolution.withColumn('evolves_to', df_chain_evolution.chain_evolution.getItem('evolves_to'))
df_chain_evolution = df_chain_evolution.withColumn('evolutionary_line', array(size(col('evolves_to')['evolves_to']), size(col('evolves_to')['evolves_to'][0])))

# COMMAND ----------

answer = df_chain_evolution.where((col('evolutionary_line')[0] > 1) | (col('evolutionary_line')[1] > 1)).count()

# COMMAND ----------

f'O número de Pokémon com mais de um caminho evolutivo é {answer}'

# COMMAND ----------

