from pyspark.sql.functions import *
from pyspark.sql.types import *

def process_inventory_df(inventory_dict):
    combined_df = None
    for inventory_month,inventory_df in inventory_dict.items():
        '''
        dropping records that do not contain atleast two non NULL values

        example records: "","","","","","","","","",""
                         "AJANTA PHARMA LTD","","","","","","","","",""
        '''
        new_df = inventory_df.na.drop(thresh=2)

        '''
        trim -> to trim the white spaces at the start and end
        filtering the unnecessary records like:

                ITEM DESCRIPTION,OPENING,"",RECEIPT,"",ISSUE,"",CLOSING,"",DUMP
                TOTAL,12439,965731.23,16882,1025495.5,13054,895144.48,16267,1233033,0
        '''
        filtered_df = new_df.filter((trim(new_df.item) != 'TOTAL') & (trim(new_df.item) != 'ITEM DESCRIPTION') \
                                    & (trim(new_df.item) != 'itemdescri'))

        # changing the data types of the columns
        mapped_df = filtered_df.withColumn('item',trim(col('item'))).withColumn('item', regexp_replace('item',' +',' ')) \
           .withColumn('opening_quantity', when(col('opening_quantity') == '-', None).otherwise(col('opening_quantity').cast(IntegerType()))) \
           .withColumn('opening_value', when(col('opening_value') == '-', None).otherwise(col('opening_value').cast(DoubleType()))) \
           .withColumn('receipt_quantity', when(col('receipt_quantity') == '-', None).otherwise(col('receipt_quantity').cast(IntegerType()))) \
           .withColumn('receipt_value', when(col('receipt_value') == '-', None).otherwise(col('receipt_value').cast(DoubleType()))) \
           .withColumn('issue_quantity', when(col('issue_quantity') == '-', None).otherwise(col('issue_quantity').cast(IntegerType()))) \
           .withColumn('issue_value', when(col('issue_value') == '-', None).otherwise(col('issue_value').cast(DoubleType()))) \
           .withColumn('closing_quantity', when(col('closing_quantity') == '-', None).otherwise(col('closing_quantity').cast(IntegerType()))) \
           .withColumn('closing_value', when(col('closing_value') == '-', None).otherwise(col('closing_value').cast(DoubleType()))) \
           .withColumn('stale_stock', when(col('stale_stock') == '-', None).otherwise(col('stale_stock').cast(IntegerType())))
    
        null_filled_df = mapped_df.na.fill(0)

        # Adding month month-year column to the dataframe
        resultant_df = null_filled_df.withColumn('inventory_month',lit(inventory_month))

        if combined_df is None:
            combined_df = resultant_df
        else:
            combined_df = combined_df.union(resultant_df)

    return combined_df.coalesce(2)

def process_products_list_df(products_list_df,inventory_df):

    concatnated_products_desc_df = products_list_df.withColumn('ITEM CODE',concat_ws(" ",products_list_df['ITEM CODE'],products_list_df['ITEM DESCRIPTION']))

    columns_filtered_df = concatnated_products_desc_df.drop('SNo.','ITEM DESCRIPTION')

    final_product_company_list_df = columns_filtered_df.withColumn('ITEM CODE',regexp_replace('ITEM CODE',' +',' ')) \
                   .withColumnRenamed('ITEM CODE', 'product_name') \
                   .withColumnRenamed('Company', 'company_name')
 
    merged_df = inventory_df.join(final_product_company_list_df, inventory_df.item == final_product_company_list_df.product_name,"left")
 
    non_matching_filtered_df = merged_df.filter(col('company_name').isNull()).drop('company_name','product_name')
 
    non_matching_filtered_df2 = non_matching_filtered_df.withColumn('item_first_name',split(col("item"), " ").getItem(0))
 
    non_matched_list_df = final_product_company_list_df.withColumn('product_first_name',split(col('product_name')," ").getItem(0))

    non_match_final_df = non_matching_filtered_df2.join(non_matched_list_df,non_matching_filtered_df2.item_first_name == non_matched_list_df.product_first_name,"inner") \
                                              .drop('item_first_name','product_name','product_first_name')
 
    final_df = merged_df.na.drop(subset=['company_name']).drop('product_name')
 
    return final_df