import os
import yaml
import re
import logging
import sys
import pandas as pd

from db_utilities import DBMaster
from tools import get_dir_files_names, align_multi_words_names
from api_utilities import CocktailExtractor
from string import ascii_lowercase

logging.basicConfig(stream=sys.stdout, level=logging.INFO)


def create_tables_from_mappings():
    dbm = DBMaster(config['db_name'])
    mappings_path = base_path + '/db_files/mapping'
    tables_to_create = get_dir_files_names(mappings_path)
    for table_name in tables_to_create:
        dbm.create_table(table_name=table_name)
    dbm.close()


def get_headers_from_mapping(table_name: str) -> list:
    headers_file_path = base_path + '/db_files/mapping/{}.tsv'.format(table_name)
    head_pat = re.compile('(^[^\\t]+)\\t.+')
    headers = []
    with open(headers_file_path, 'r') as header_file:
        next(header_file)
        for line in header_file:
            headers.append(head_pat.match(line).group(1))
    return headers


def insert_initial_drink_data(drinks: list):
    headers = get_headers_from_mapping(table_name='drinks')
    data = pd.DataFrame(columns=headers)
    for i_drink in drinks:
        ins_values = [i_drink['idDrink'], i_drink['strDrink'], i_drink['strDrinkAlternate'], i_drink['strDrinkES'],
                      i_drink['strDrinkDE'], i_drink['strDrinkFR'], i_drink['strDrinkZH-HANS'],
                      i_drink['strDrinkZH-HANT'], None, i_drink['strIBA'],
                      1 if i_drink['strAlcoholic'] == 'Alcoholic' else 0, None, i_drink['strCreativeCommonsConfirmed'],
                      i_drink['dateModified']]
        data.loc[len(data) + 1] = ins_values
    dbm = DBMaster(config['db_name'])
    dbm.insert_df_into_table(table_name='drinks', data=data)
    dbm.close()


def get_drink_ids() -> dict:
    dbm = DBMaster(config['db_name'])
    drink_ids_query = 'SELECT id, name from {}'
    drinks_ids_raw = dbm.select_from_table(table='drinks', query=drink_ids_query)
    dbm.close()
    ids = {}
    for d_id in drinks_ids_raw:
        ids[d_id[1]] = d_id[0]
    return ids


def create_tags():
    dbm = DBMaster(config['db_name'])
    dbm.insert_column_into_table(table_name='tags', column='value', values=tags)
    tags_id_query = 'SELECT id, value FROM {};'
    raw_tags_ids = dbm.select_from_table(table='tags', query=tags_id_query)
    tags_ids = {}
    for tid in raw_tags_ids:
        tags_ids[tid[1]] = tid[0]
    headers = get_headers_from_mapping(table_name='drink_tags')
    data = pd.DataFrame(columns=headers)
    for t_drink in drinks_map:
        if drinks_map[t_drink]['tags']:
            for tag in drinks_map[t_drink]['tags'].split(','):
                ins_vals = [drinks_map[t_drink]['id'], tags_ids[tag]]
                data.loc[len(data) + 1] = ins_vals
    dbm.insert_df_into_table(table_name='drink_tags', data=data)
    dbm.close()


def create_media():
    headers = get_headers_from_mapping(table_name='media')
    data = pd.DataFrame(columns=headers)
    for m_drink in drinks_map:
        ins_values = [drinks_map[m_drink]['id']] + drinks_map[m_drink]['media']
        data.loc[len(data) + 1] = ins_values
    dbm = DBMaster(config['db_name'])
    dbm.insert_df_into_table(table_name='media', data=data)
    dbm.close()


def create_ingredients():
    dbm = DBMaster(config['db_name'])
    dbm.insert_column_into_table(table_name='ingredients', column='name', values=ingredients)
    ingr_ids_query = 'SELECT id, name FROM {};'
    raw_ingr_ids = dbm.select_from_table(table='ingredients', query=ingr_ids_query)
    ingr_ids = {}
    for iid in raw_ingr_ids:
        ingr_ids[iid[1]] = iid[0]
    headers = get_headers_from_mapping(table_name='measures')
    data = pd.DataFrame(columns=headers)
    for ii_drink in drinks_map:
        for ingr in drinks_map[ii_drink]['ingredients']:
            ins_values = [drinks_map[ii_drink]['id'], ingr_ids[ingr], drinks_map[ii_drink]['ingredients'][ingr]]
            data.loc[len(data) + 1] = ins_values
    dbm.insert_df_into_table(table_name='measures', data=data)
    dbm.close()


def create_glasses():
    dbm = DBMaster(config['db_name'])
    dbm.insert_column_into_table(table_name='glasses', column='name', values=glasses)
    gls_id_query = 'SELECT id, name FROM {};'
    raw_gls_ids = dbm.select_from_table(table='glasses', query=gls_id_query)
    gls_ids = {}
    logging.info(msg="Updating glasses references")
    for gid in raw_gls_ids:
        gls_ids[gid[1]] = gid[0]
    upd_query = 'UPDATE drinks SET glass_id = {0} WHERE id = {1};'
    for g_drink in drinks_map:
        dbm.execute_dml(upd_query.format(gls_ids[drinks_map[g_drink]['glass']], drinks_map[g_drink]['id']))
    dbm.close()


def create_categories():
    dbm = DBMaster(config['db_name'])
    dbm.insert_column_into_table(table_name='categories', column='value', values=categories)
    cat_id_query = 'SELECT id, value FROM {};'
    raw_cat_ids = dbm.select_from_table(table='categories', query=cat_id_query)
    cat_ids = {}
    for cid in raw_cat_ids:
        cat_ids[cid[1]] = cid[0]
    logging.info(msg="Updating categories references")
    upd_query = 'UPDATE drinks SET category_id = {0} WHERE id = {1};'
    for c_drink in drinks_map:
        dbm.execute_dml(upd_query.format(cat_ids[drinks_map[c_drink]['category']], drinks_map[c_drink]['id']))
    dbm.close()


def create_instructions():
    dbm = DBMaster(config['db_name'])
    headers = get_headers_from_mapping(table_name='instructions')
    data = pd.DataFrame(columns=headers)
    for in_drink in drinks_map:
        ins_values = [drinks_map[in_drink]['id']] + drinks_map[in_drink]['instructions']
        data.loc[len(data) + 1] = ins_values
    dbm.insert_df_into_table(table_name='instructions', data=data)
    dbm.close()


if __name__ == '__main__':

    # set initial variables
    base_path = os.path.dirname(__file__)
    config_path = base_path + '/config.yaml'
    with open(config_path, 'r') as stream:
        config = yaml.safe_load(stream)
    logging.info(msg="Initiated. Starting processing")

    # create all tables described in mapping
    logging.info(msg="DB and tables created")
    create_tables_from_mappings()

    # extract all cocktails data by their first letters
    api_url = config['api_url']
    token = config['api_token']
    queries = config['queries']
    c_extr = CocktailExtractor(api_url=api_url, token=token, queries=queries)
    drinks_data = []
    logging.info(msg="Getting API data")
    for letter in ascii_lowercase:
        list_data = c_extr.c_get_list_by_first_letter(letter=letter)
        if list_data:
            # filter data with German instructions
            for record in list_data:
                if record['strInstructionsDE']:
                    drinks_data.append(record)
    logging.info(msg="API data fetched successfully")

    # load base drinks data to db
    logging.info(msg="Loading initial drinks data")
    insert_initial_drink_data(drinks=drinks_data)

    # fetch drinks table index
    logging.info(msg="Fetching drinks IDs")
    drinks_ids = get_drink_ids()

    # create convenient drinks map
    logging.info(msg="Creating drinks map")
    nu_tags = []
    nu_ingredients = []
    nu_glasses = []
    nu_categories = []
    drinks_map = {}
    for drink in drinks_data:
        drinks_map[drink['strDrink']] = {}
        drinks_map[drink['strDrink']]['id'] = drinks_ids[drink['strDrink']]
        # media
        drinks_map[drink['strDrink']]['media'] = [drink['strVideo'], drink['strDrinkThumb'], drink['strImageSource'],
                                                  drink['strImageAttribution']]
        # tags
        drinks_map[drink['strDrink']]['tags'] = drink['strTags']
        if drink['strTags']:
            nu_tags += drink['strTags'].split(',')
        # ingredients
        drinks_map[drink['strDrink']]['ingredients'] = {}
        for i in range(1, 16):
            if drink['strIngredient{}'.format(i)] and drink['strMeasure{}'.format(i)]:

                # unify ingredients shape
                fixed_ingr = align_multi_words_names(drink['strIngredient{}'.format(i)])

                drinks_map[drink['strDrink']]['ingredients'][fixed_ingr] = drink['strMeasure{}'.format(i)]
                nu_ingredients.append(fixed_ingr)
        # glasses
        fixed_glass = align_multi_words_names(drink['strGlass'])
        drinks_map[drink['strDrink']]['glass'] = fixed_glass
        if drink['strGlass']:
            nu_glasses.append(fixed_glass)
        # categories
        drinks_map[drink['strDrink']]['category'] = drink['strCategory']
        if drink['strCategory']:
            nu_categories.append(drink['strCategory'])
        # instructions
        drinks_map[drink['strDrink']]['instructions'] = [drink['strInstructions'], drink['strInstructionsES'],
                                                         drink['strInstructionsDE'], drink['strInstructionsFR'],
                                                         drink['strInstructionsZH-HANS'],
                                                         drink['strInstructionsZH-HANT']]

    # process and load tags data
    # to decrease data to load, filter only records with available tags
    logging.info(msg="Processing tags data")
    d_tags = {}
    for drink in drinks_map:
        if drinks_map[drink]['tags']:
            d_tags[drinks_map[drink]['id']] = drinks_map[drink]['tags']
    tags = list(set(nu_tags))
    create_tags()

    # process and load meadia data
    logging.info(msg="Processing media data")
    create_media()

    # process and load tags data
    logging.info(msg="Processing ingredients data")
    ingredients = list(set(nu_ingredients))
    create_ingredients()

    # process and load glasses data
    logging.info(msg="Processing glasses data")
    glasses = list(set(nu_glasses))
    create_glasses()

    # process and load categories data
    logging.info(msg="Processing categories data")
    categories = list(set(nu_categories))
    create_categories()

    # process and load instructions data
    logging.info(msg="Processing instructions data")
    create_instructions()

    logging.info(msg="Processing complete")
    logging.info(msg="Prost!")
