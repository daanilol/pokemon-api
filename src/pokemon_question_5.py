import requests
import json
import pyspark
from sparksession import spark
from pyspark.sql.functions import col, udf, explode, size, array, greatest, when
from pyspark.sql.types import StructType, StructField, MapType, StringType, IntegerType, ArrayType, BooleanType, NullType

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

@udf(returnType=schema)
def get_pokemon_json(x):
    return json.loads(requests.get(x).text)

pokemon_json = json.loads(requests.get('https://pokeapi.co/api/v2/pokemon/?limit=-1').text)['results']
df_pokemon = spark.createDataFrame(pokemon_json)

df_pokemon = df_pokemon.withColumn('pokemon_json', get_pokemon_json(col('url')))
df_pokemon = df_pokemon.withColumn('is_default', col('pokemon_json')['is_default'])

chain_json = json.loads(requests.get('https://pokeapi.co/api/v2/evolution-chain/?limit=-1').text)['results']
df_chain = spark.createDataFrame(chain_json)

chain_schema_2 = ArrayType(StructType([
    StructField('evolution_details', ArrayType(NullType())),
    StructField('evolves_to', StringType()),
    StructField('is_baby', StringType()),
    StructField('species', MapType(StringType(), StringType()))
]))

chain_schema_1 = ArrayType(StructType([
    StructField('evolution_details', ArrayType(NullType())),
    StructField('evolves_to', chain_schema_2),
    StructField('is_baby', StringType()),
    StructField('species', MapType(StringType(), StringType()))
]))

chain_schema_0 = StructType([
    StructField('evolution_details', StringType()),
    StructField('evolves_to', chain_schema_1),
    StructField('is_baby', StringType()),
    StructField('species', MapType(StringType(), StringType()))
])

chain_json_schema = StructType([
    StructField('baby_trigger_item', NullType()),
    StructField('chain', chain_schema_0),
    StructField('id', StringType())
])

@udf(returnType=chain_json_schema)
def get_chain_json(url):
    return json.loads(requests.get(url).text)
  
df_chain = df_chain.withColumn('chain_json', get_chain_json(col('url')))

df_pokemon = df_pokemon.withColumn('is_default', col('pokemon_json')['is_default'])
#df_pokemon = df_pokemon.withColumn('id', col('pokemon_json')['id'].cast(IntegerType()))

@udf(returnType = ArrayType(StringType()))
def get_name_first_evolution(x):
    list_name = list()
    for item in x:
        for other in item:
            list_name.append(other['species']['name'])
    return list_name

df_chain = df_chain.withColumn('pokemon', col('chain_json')['chain']['species']['name'])
df_chain = df_chain.join(df_pokemon, df_chain.pokemon == df_pokemon.name, 'inner')

df_chain = df_chain.withColumn('pokemon', col('chain_json')['chain']['species']['name'])
df_chain = df_chain.withColumn('pokemon', col('chain_json')['chain']['species']['name'])

df_chain = df_chain.withColumn('info_first_evo', col('chain_json')['chain']['evolves_to'])
df_chain = df_chain.where(size(col('info_first_evo')) >= 1)
df_chain = df_chain.withColumn('id', col('chain_json')['id'].cast(IntegerType()))
df_chain = df_chain.withColumn('first_evo_to_explode', get_name_first_evolution(array(col('info_first_evo'))))
df_chain = df_chain.withColumn('first_evo', explode(col('first_evo_to_explode'))).select('id', 'pokemon', 'first_evo', 'info_first_evo', 'chain_json')

df_chain = df_chain.withColumn('info_second_evo', col('info_first_evo')['evolves_to'])
df_chain = df_chain.withColumn('second_evo', get_name_first_evolution(col('info_second_evo'))).select('id', 'pokemon', 'first_evo', 'second_evo')#

no_second_pokemon_first = df_chain.where(size(col('second_evo')) < 1)
has_second_pokemon_first = df_chain.where(size(col('second_evo')) >= 1)
no_second_pokemon_first = no_second_pokemon_first.select('id', no_second_pokemon_first.pokemon.alias('pre_evo'), no_second_pokemon_first.first_evo.alias('evolve'))
compare_pre_first = has_second_pokemon_first.select('id', has_second_pokemon_first.pokemon.alias('pre_evo'), has_second_pokemon_first.first_evo.alias('evolve'))
compare_first_second = has_second_pokemon_first.select('id', has_second_pokemon_first.first_evo.alias('pre_evo'), explode(col('second_evo')).alias('evolve'))

df_unified = compare_pre_first.union(compare_first_second)
df_unified = df_unified.union(no_second_pokemon_first)

df_unified = df_unified.join(df_pokemon, df_unified.pre_evo == df_pokemon.name, "left")
df_unified = df_unified.withColumnRenamed('name', 'name_del')
df_unified = df_unified.withColumnRenamed('pokemon_json', 'pre_evo_json')
df_unified = df_unified.withColumnRenamed('is_default', 'pre_evo_is_default')
df_unified = df_unified.join(df_pokemon, df_unified.evolve == df_pokemon.name, "left")
df_unified = df_unified.withColumnRenamed('pokemon_json', 'evolve_json')
df_unified = df_unified.where(col('pre_evo_is_default') == True)
df_unified = df_unified.where(col('is_default') == True)

df_unified = df_unified.withColumn('pre_evo_hp', df_unified['pre_evo_json']['stats'][0]['base_stat'])\
      .withColumn('pre_evo_attack', df_unified['pre_evo_json']['stats'][1]['base_stat'])\
      .withColumn('pre_evo_defense', df_unified['pre_evo_json']['stats'][2]['base_stat'])\
      .withColumn('pre_evo_special_attack', df_unified['pre_evo_json']['stats'][3]['base_stat'])\
      .withColumn('pre_evo_special_defense', df_unified['pre_evo_json']['stats'][4]['base_stat'])\
      .withColumn('pre_evo_speed', df_unified['pre_evo_json']['stats'][5]['base_stat'])

df_unified = df_unified.withColumn('evolve_hp', df_unified['evolve_json']['stats'][0]['base_stat'])\
      .withColumn('evolve_attack', df_unified['evolve_json']['stats'][1]['base_stat'])\
      .withColumn('evolve_defense', df_unified['evolve_json']['stats'][2]['base_stat'])\
      .withColumn('evolve_special_attack', df_unified['evolve_json']['stats'][3]['base_stat'])\
      .withColumn('evolve_special_defense', df_unified['evolve_json']['stats'][4]['base_stat'])\
      .withColumn('evolve_speed', df_unified['evolve_json']['stats'][5]['base_stat'])


df_unified = df_unified.select('id', 'pre_evo', 'evolve', 'pre_evo_hp', 'pre_evo_attack', 'pre_evo_defense',\
                  'pre_evo_special_attack', 'pre_evo_special_defense',\
                  'pre_evo_speed', 'evolve_hp', 'evolve_attack', 'evolve_defense',\
                  'evolve_special_attack', 'evolve_special_defense', 'evolve_speed')


df_unified = df_unified.withColumn('calc_hp', col('evolve_hp') - col('pre_evo_hp'))\
          .withColumn('calc_attack', col('evolve_attack') - col('pre_evo_attack'))\
          .withColumn('calc_defense', col('evolve_defense') - col('pre_evo_defense'))\
          .withColumn('calc_special_attack', col('evolve_special_attack') - col('pre_evo_special_attack'))\
          .withColumn('calc_special_defense', col('evolve_special_defense') - col('pre_evo_special_defense'))\
          .withColumn('calc_speed', col('evolve_speed') - col('pre_evo_speed'))\
          .select('id', 'pre_evo', 'evolve', col('calc_hp').alias('hp'), col('calc_attack').alias('attack'),\
                  col('calc_defense').alias('defense'), col('calc_special_attack').alias('special_attack'),\
                  col('calc_special_defense').alias('special_defense'), col('calc_speed').alias('speed'))

df_unified = df_unified.withColumn("value", greatest('hp', 'attack', 'defense', 'special_attack', 'special_defense', 'speed'))

df_unified = df_unified.withColumn('stats', when(col('hp') == col('value'), 'hp') \
                                .when(col('attack') == col('value'), 'attack') \
                                .when(col('defense') == col('value'), 'defense') \
                                .when(col('special_attack') == col('value'), 'special_attack') \
                                .when(col('special_defense') == col('value'), 'special_defense') \
                                .when(col('speed') == col('value'), 'speed')).select('pre_evo', 'evolve', 'stats', 'value')

df_unified.orderBy(col('value').desc()).show(7)