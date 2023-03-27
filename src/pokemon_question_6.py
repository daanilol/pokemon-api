import requests
import pyspark
from sparksession import spark
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StructType, StructField, MapType, StringType, IntegerType, ArrayType, BooleanType, NullType

stats_schema = ArrayType(StructType(
    [
        StructField('base_stat', IntegerType()),
        StructField('effort', StringType()),
        StructField('stat', MapType(StringType(), StringType()))
        ]
    )
)

types_schema = ArrayType(StructType([
        StructField('slot', IntegerType()),
        StructField('type', MapType(StringType(), StringType()))
        ]
    )
)

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
        StructField('types', types_schema),
        StructField('weight', StringType())        
    ])

@udf(returnType=schema)
def get_json(x):
    return requests.get(x).json()

df_pokemon = spark.createDataFrame(requests.get('https://pokeapi.co/api/v2/pokemon/?limit=-1').json()['results'])

df_pokemon = df_pokemon.withColumn('pokemon_json', get_json(col('url')))

df_pokemon = df_pokemon.withColumn('type_1', col('pokemon_json')['types'][0]['type']['name'])\
                       .withColumn('type_2', col('pokemon_json')['types'][1]['type']['name'])\
                       .withColumn('url_type_1', col('pokemon_json')['types'][0]['type']['url'])\
                       .withColumn('url_type_2', col('pokemon_json')['types'][1]['type']['url'])

df_pokemon = df_pokemon.withColumn('is_default', col('pokemon_json')['is_default'])
df_pokemon = df_pokemon = df_pokemon.where(col('is_default') == True)

schema_damage_relations = StructType([
    StructField('double_damage_from', StringType()),
    StructField('double_damage_to', ArrayType(MapType(StringType(), StringType()))),
    StructField('half_damage_from', StringType()),
    StructField('half_damage_to', StringType()),
    StructField('no_damage_from', StringType()),
    StructField('no_damage_to', StringType())
])

schema_types = StructType([
    StructField('damage_relations', schema_damage_relations),
    StructField('game_indices', StringType()),
    StructField('generation', StringType()),
    StructField('id', StringType()),
    StructField('move_damage_class', StringType()),
    StructField('moves', StringType()),
    StructField('name', StringType()),
    StructField('names', StringType()),
    StructField('past_damage_relations', StringType()),
    StructField('pokemon', StringType())
])

@udf(returnType=schema_types)
def get_json_to_damage(x):
    if x == None:
        return None
    else:
        return requests.get(x).json()


@udf(returnType=ArrayType(StringType()))
def get_advantage_types(x):
    list_types = list()
    
    if x == None:
        return list('')
    
    else:
        for item in x:
            list_types.append(item['name'])
        
    return list_types

df_pokemon = df_pokemon.withColumn('types_json_1', get_json_to_damage(col('url_type_1')))\
                       .withColumn('types_json_2', get_json_to_damage(col('url_type_2')))

df_pokemon = df_pokemon.withColumn('advantage_list_1', col('types_json_1')['damage_relations']['double_damage_to'])\
                       .withColumn('advantage_list_2', col('types_json_2')['damage_relations']['double_damage_to'])

df_pokemon = df_pokemon.withColumn('advantages_1', get_advantage_types(col('advantage_list_1')))\
                       .withColumn('advantages_2', get_advantage_types(col('advantage_list_2')))\
                       .select(col('name').alias('pokemon'), 'type_1', 'type_2', 'advantages_1', 'advantages_2')

collect_types = df_pokemon.select('type_1', 'type_2').collect()

def get_type_list(collect_types):
    types_list = list()
    for item in collect_types:
        types_list.append(item[0])
        types_list.append(item[1])
    return types_list

types_list_complete = get_type_list(collect_types)


def get_freq_of_types(types_list_complete):
    freq = dict()
    for item in types_list_complete:
        if item in freq:
            freq[item] += 1
        else:
            freq[item] = 1

    freq.pop(None)
    return freq

freq_complete = get_freq_of_types(types_list_complete)

@udf(returnType=IntegerType())
def get_number_of_pokemon_advantage(x):
    value = 0 
    for item in x:
        if item in freq_complete:
            value += freq_complete[item]
    return value

df_pokemon = df_pokemon.withColumn('quant_advantage_1', get_number_of_pokemon_advantage(col('advantages_1')))\
                       .withColumn('quant_advantage_2', get_number_of_pokemon_advantage(col('advantages_2')))

df_pokemon.withColumn('value', col('quant_advantage_1') + col('quant_advantage_2'))\
          .select('pokemon', 'value')\
          .orderBy(col('value').desc()).show(7)