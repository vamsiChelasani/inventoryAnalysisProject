import sys
from lib import dataManipulation, dataReader, utils, transformations
from pyspark.sql.functions import *

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Please specify the environment")
        sys.exit(-1)

    job_run_env = sys.argv[1]

    print("Creating Spark Session")

    spark = utils.get_spark_session(job_run_env)

    print("Created Spark Session")

    inventory_dict = dataReader.read_inventory_data(spark,job_run_env)

    inventory_df = dataManipulation.process_inventory_df(inventory_dict)

    products_list_df = dataReader.read_products_list(spark,job_run_env)

    final_df = dataManipulation.process_products_list_df(products_list_df,inventory_df)

    #final_df.show()
    total_records = final_df.count()
    
    transformations.transformAndSaveDF(spark, final_df)

    print("end of main: ",total_records)