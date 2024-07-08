import os

from lib import configReader

#defining monthly inventory schema
def get_inventory_schema():
    inv_schema = "item string,opening_quantity string, opening_value string, receipt_quantity string, \
                  receipt_value string, issue_quantity string, issue_value string, closing_quantity string, \
                  closing_value string, stale_stock string"
    return inv_schema

# creating inventory dataframe
def read_inventory_data(spark,env):

    combined_df = {}
    conf = configReader.get_app_config(env)
    file_path = conf["inventory.file.path"]
    file_list = [f for f in os.listdir(file_path) if f.endswith('.csv')]
 
    for file_name in file_list:

        base_name = os.path.basename(file_name)
        month_year = os.path.splitext(base_name)[0]

        inv_file_path = file_path + file_name
        raw_df = spark.read \
           .format("csv") \
           .option("inferSchema", "false") \
           .schema(get_inventory_schema()) \
           .load(inv_file_path)

        combined_df[month_year] = raw_df
 
    return combined_df

#creating products dataframe
def read_products_list(spark,env):
    conf = configReader.get_app_config(env)
    products_file_path = conf["products.file.path"]
    return spark.read \
                .format("csv") \
                .option("header", "true") \
                .load(products_file_path)