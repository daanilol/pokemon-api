import requests
import json
from sparksession import spark
from pyspark.sql.functions import col, udf, max as _max
from pyspark.sql.types import StructType, StructField, StringType, MapType, BooleanType, IntegerType, ArrayType

archive = json.loads(requests.get('https://pokeapi.co/api/v2/pokemon/?limit=-1').text)['results']

schema = StructType([
    StructField('name', StringType()),
    StructField('url', StringType())
])

df_pokemon_moves = spark.createDataFrame(archive, schema)

stats_schema = ArrayType(StructType([
    StructField('base_stat', IntegerType()),
    StructField('effort', IntegerType()),
    StructField('stat', MapType(StringType(), StringType()))
]))

version_group_schema = ArrayType(StructType([
    StructField('level_learned_at', IntegerType()),
    StructField('move_learn_method', MapType(StringType(), StringType())),
    StructField('version_group', MapType(StringType(), StringType()))
]))

moves_schema = ArrayType(StructType([
    StructField('move', MapType(StringType(), StringType())),
    StructField('version_group_details', version_group_schema)
]))

json_schema = StructType([
    StructField('abilities', StringType()),
    StructField('base_experience', IntegerType()),
    StructField('forms', StringType()),
    StructField('game_indices', StringType()),
    StructField('height', StringType()),
    StructField('held_items', StringType()),
    StructField('id', IntegerType()),
    StructField('is_default', BooleanType()),
    StructField('location_area_encounters', StringType()),
    StructField('moves', moves_schema),
    StructField('name', StringType()),
    StructField('order', StringType()),
    StructField('past_types', StringType()),
    StructField('species', StringType()),
    StructField('sprites', StringType()),
    StructField('stats', stats_schema),
    StructField('types', StringType()),
    StructField('weight', StringType())    
])

@udf(returnType=json_schema)
def get_pokemon(x):
    return json.loads(requests.get(x).text)


@udf(returnType=ArrayType(StringType()))
def get_moves_lvl_up(x):
    moves_name_list = list()
    for item in x:
        for other_item in item['version_group_details']:
            if other_item['move_learn_method']['name'] == 'level-up':
                moves_name_list.append(item['move']['name'])  
    return moves_name_list

df_pokemon_moves = df_pokemon_moves.withColumn('pokemon_json', get_pokemon(col('url')))
df_pokemon_moves = df_pokemon_moves.withColumn('is_default', df_pokemon_moves.pokemon_json \
                                               .getItem('is_default'))

df_pokemon_moves = df_pokemon_moves.withColumn('moves', \
                                               df_pokemon_moves.pokemon_json.getItem('moves'))

df_pokemon_moves = df_pokemon_moves.withColumn('stat_attack', \
                                               df_pokemon_moves.pokemon_json\
                                               .getItem('stats')[1]['base_stat'])\
                                               .select('name', 'is_default', 'stat_attack', 'moves')

df_pokemon_moves = df_pokemon_moves.where(df_pokemon_moves.is_default == True)
df_pokemon_moves = df_pokemon_moves.withColumn('lvl_method_moves', get_moves_lvl_up(col('moves')))


def get_most_learned_move():
    lvl_method_moves_collect = df_pokemon_moves.select('lvl_method_moves').collect()
    freq = dict()
    for item in lvl_method_moves_collect:
        for other_item in item[0]:
            if other_item in freq:
                freq[other_item] += 1
            else:
                freq[other_item] = 1

    return max(freq, key=freq.get)

most_learned_move = get_most_learned_move()

df_pokemon_moves.show()