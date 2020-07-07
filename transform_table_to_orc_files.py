import click
from pyspark.context import SparkContext
from awsglue.context import GlueContext
# TODO: make imports explicit.  Do not use glob include here.
from awsglue.transforms import * 


def replace_dots(string):
    '''replace dots with underscore in string'''
    return string.replace('.', '_')


def get_dataframe_collection_from_athena_table(db_name, table_name, source_s3_object_path):
    '''
    Use relationalize function to flatten the data by creating individual data
    frame for each array associated with the main dynamic dataframe network_data.
    These dataframes are collectively grouped into one data frame collection
    (dfc).
    '''

    glueContext = GlueContext(SparkContext.getOrCreate())
    catalog_data = glueContext.create_dynamic_frame.from_catalog(
        database=db_name,
        table_name=table_name,
    )
    #catalog_data.printSchema()

    dfc = catalog_data.relationalize("data_root", "s3://" + source_s3_object_path + "/")
    #print(dfc.keys())
    return dfc


def load_dataframe_as_orc_file_to_s3(df_name, bucket_name):
    '''
    Write dynamic frame into s3 bucket with the same name as dataframe.
    The content of the file in the bucket is in the form of orc file.
    '''
    df = dfc.select(df_name)
    response = glueContext.write_dynamic_frame.from_options(
        frame = df,
        connection_type = "s3",
        connection_options = {"path": "s3://" + bucket_name + "/" + replace_dots(df_name) + "/"},
        format = "orc",
        transformation_ctx = replace_dots(df_name),
    )


# TODO: add click paramater parsing here, with descriptions of parameters
def main(source_db_name, source_table_name, source_s3_object_path, target_bucket_name, df_name):
    dfc = get_dataframe_collection_from_athena_table(source_db_name, source_table_name, source_s3_object_path)
    load_dataframe_as_orc_file_to_s3("data_root", target_bucket_name)
    for df_name in dfc.keys():
        load_dataframe_as_orc_file_to_s3(df_name, target_bucket_name)


if __name__ == "__main__":
    
    main()

