# Import aws glue libraries below

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# network_data_table is a athena table generated from nested json file.


# Create dynamic frame from catalog table network_data_table in database org_data. We
# can query the dataframe using printSchema

glueContext = GlueContext(SparkContext.getOrCreate())
network_data = glueContext.create_dynamic_frame.from_catalog(
    database="org_data",
    table_name="network_data_table"
)
network_data.printSchema()


'''Use relationalize function to flatten the data by creating individual data
frame for each array associated with the main dynamic dataframe network_data.
These dataframes are collectively grouped into one data frame collection
(dfc).'''

dfc = network_data.relationalize("network_data_root", "s3://rohit-glue-bucket/network_data_orc/temp-dir/")
dfc.keys()

'''
Commands below help select each dataframe from dataframe group.  Since
athena database doesn't accept the object with multiple '.' we have to replace
'.' with'_' in the dataframe name.  Also the underlying columns in the
dataframe have .  which need to be replaced by _ otherwise those columns cannot
be selected individually in sql query.  We need to figure out a way to replace
'.' with '_' programitically using withColumnRenamed function in pyspark.

Below commands writes dynamic frame into s3 bucket with the same name as
dataframe. The content of the file in the bucket is in the form of orc file.
'''

bucket_name = 'rohit-glue-bucket'
def rename(df):
    ''' replace dots with underscore'''


for df in drc.key():
    df_name = rename(df)
    write_dataframe(df_name) 

network_data_root_account_attached_policy_ids = dfc.select('network_data_root_account.attached_policy_ids')
network_data_root_account_attached_policy_ids = glueContext.write_dynamic_frame.from_options(
    frame = network_data_root_account_attached_policy_ids,
    connection_type = "s3",
    connection_options = {"path": "s3://rohit-glue-bucket/network_data_root_account_attached_policy_ids/"},
    format = "orc",
    transformation_ctx = "network_data_root_account_attached_policy_ids"
)



network_data_root = dfc.select('network_data_root')
network_data_root = glueContext.write_dynamic_frame.from_options(
    frame = network_data_root,
    connection_type = "s3",
    connection_options = {"path": "s3://rohit-glue-bucket/network_data_root/"},
    format = "orc",
    transformation_ctx = "network_data_root"
)


network_data_root_payload_output_Vpcs = dfc.select('network_data_root_payload_output.Vpcs')
network_data_root_payload_output_Vpcs = glueContext.write_dynamic_frame.from_options(
    frame = network_data_root_payload_output_Vpcs,
    connection_type = "s3",
    connection_options = {"path": "s3://rohit-glue-bucket/network_data_root_payload_output_Vpcs/"},
    format = "orc",
    transformation_ctx = "network_data_root_payload_output_Vpcs"
)


network_data_root_account_attached_policy_ids = dfc.select('network_data_root_account.attached_policy_ids')
network_data_root_account_attached_policy_ids = glueContext.write_dynamic_frame.from_options(
    frame = network_data_root_account_attached_policy_ids,
    connection_type = "s3",
    connection_options = {"path": "s3://rohit-glue-bucket/network_data_root_account_attached_policy_ids/"},
    format = "orc",
    transformation_ctx = "network_data_root_account_attached_policy_ids"
)




network_data_root_payload_output_InternetGateways_Attachments=dfc.select('network_data_root_payload_output.InternetGateways.val.Attachments')
network_data_root_payload_output_Vpcs_Tags=dfc.select('network_data_root_payload_output.Vpcs.val.Tags')
network_data_root_payload_output_RouteTables=dfc.select('network_data_root_payload_output.RouteTables')
network_data_root_payload_output_InternetGateways_Tags=dfc.select('network_data_root_payload_output.InternetGateways.val.Tags')
network_data_root_payload_output_InternetGateways=dfc.select('network_data_root_payload_output.InternetGateways')
network_data_root_payload_output_Subnets_Ipv6CidrBlockAssociationSet=dfc.select('network_data_root_payload_output.Subnets.val.Ipv6CidrBlockAssociationSet')
network_data_root_payload_output_NatGateways=dfc.select('network_data_root_payload_output.NatGateways')
network_data_root_account_aliases=dfc.select('network_data_root_account.aliases')
network_data_root_payload_output_Subnets=dfc.select('network_data_root_payload_output.Subnets')
network_data_root_payload_output_RouteTables_Associations=dfc.select('network_data_root_payload_output.RouteTables.val.Associations')
network_data_root_payload_output_RouteTables_Routes=dfc.select('network_data_root_payload_output.RouteTables.val.Routes')
network_data_root_payload_output_Vpcs_CidrBlockAssociationSet=dfc.select('network_data_root_payload_output.Vpcs.val.CidrBlockAssociationSet')
network_data_root_payload_output_RouteTables_PropagatingVgws=dfc.select('network_data_root_payload_output.RouteTables.val.PropagatingVgws')
network_data_root_payload_output_RouteTables_Tags=dfc.select('network_data_root_payload_output.RouteTables.val.Tags')




network_data_root_account_aliases = glueContext.write_dynamic_frame.from_options(frame = network_data_root_account_aliases, connection_type = "s3", connection_options = {"path": "s3://rohit-glue-bucket/network_data_root_account_aliases/"}, format = "orc", transformation_ctx = "network_data_root_account_aliases")



network_data_root_payload_output_Vpcs_Tags = glueContext.write_dynamic_frame.from_options(frame = network_data_root_payload_output_Vpcs_Tags, connection_type = "s3", connection_options = {"path": "s3://rohit-glue-bucket/network_data_root_payload_output_Vpcs_Tags/"}, format = "orc", transformation_ctx = "network_data_root_payload_output_Vpcs_Tags")

network_data_root_payload_output_Vpcs_CidrBlockAssociationSet = glueContext.write_dynamic_frame.from_options(frame = network_data_root_payload_output_Vpcs_CidrBlockAssociationSet, connection_type = "s3", connection_options = {"path": "s3://rohit-glue-bucket/network_data_root_payload_output_Vpcs_CidrBlockAssociationSet/"}, format = "orc", transformation_ctx = "network_data_root_payload_output_Vpcs_CidrBlockAssociationSet")

network_data_root_payload_output_InternetGateways = glueContext.write_dynamic_frame.from_options(frame = network_data_root_payload_output_InternetGateways, connection_type = "s3", connection_options = {"path": "s3://rohit-glue-bucket/network_data_root_payload_output_InternetGateways/"}, format = "orc", transformation_ctx = "network_data_root_payload_output_InternetGateways")

network_data_root_payload_output_InternetGateways_Attachments = glueContext.write_dynamic_frame.from_options(frame = network_data_root_payload_output_InternetGateways_Attachments, connection_type = "s3", connection_options = {"path": "s3://rohit-glue-bucket/network_data_root_payload_output_InternetGateways_Attachments/"}, format = "orc", transformation_ctx = "network_data_root_payload_output_InternetGateways_Attachments")

network_data_root_payload_output_InternetGateways_Tags = glueContext.write_dynamic_frame.from_options(frame = network_data_root_payload_output_InternetGateways_Tags, connection_type = "s3", connection_options = {"path": "s3://rohit-glue-bucket/network_data_root_payload_output_InternetGateways_Tags/"}, format = "orc", transformation_ctx = "network_data_root_payload_output_InternetGateways_Tags")

network_data_root_payload_output_RouteTables = glueContext.write_dynamic_frame.from_options(frame = network_data_root_payload_output_RouteTables, connection_type = "s3", connection_options = {"path": "s3://rohit-glue-bucket/network_data_root_payload_output_RouteTables/"}, format = "orc", transformation_ctx = "network_data_root_payload_output_RouteTables")

network_data_root_payload_output_RouteTables_Associations = glueContext.write_dynamic_frame.from_options(frame = network_data_root_payload_output_RouteTables_Associations, connection_type = "s3", connection_options = {"path": "s3://rohit-glue-bucket/network_data_root_payload_output_RouteTables_Associations/"}, format = "orc", transformation_ctx = "network_data_root_payload_output_RouteTables_Associations")

network_data_root_payload_output_RouteTables_Routes = glueContext.write_dynamic_frame.from_options(frame = network_data_root_payload_output_RouteTables_Routes, connection_type = "s3", connection_options = {"path": "s3://rohit-glue-bucket/network_data_root_payload_output_RouteTables_Routes/"}, format = "orc", transformation_ctx = "network_data_root_payload_output_RouteTables_Routes")

network_data_root_payload_output_RouteTables_PropagatingVgws = glueContext.write_dynamic_frame.from_options(frame = network_data_root_payload_output_RouteTables_PropagatingVgws, connection_type = "s3", connection_options = {"path": "s3://rohit-glue-bucket/network_data_root_payload_output_RouteTables_PropagatingVgws/"}, format = "orc", transformation_ctx = "network_data_root_payload_output_RouteTables_PropagatingVgws")

network_data_root_payload_output_RouteTables_Tags = glueContext.write_dynamic_frame.from_options(frame = network_data_root_payload_output_RouteTables_Tags, connection_type = "s3", connection_options = {"path": "s3://rohit-glue-bucket/network_data_root_payload_output_RouteTables_Tags/"}, format = "orc", transformation_ctx = "network_data_root_payload_output_RouteTables_Tags")

network_data_root_payload_output_Subnets = glueContext.write_dynamic_frame.from_options(frame = network_data_root_payload_output_Subnets, connection_type = "s3", connection_options = {"path": "s3://rohit-glue-bucket/network_data_root_payload_output_Subnets/"}, format = "orc", transformation_ctx = "network_data_root_payload_output_Subnets")

network_data_root_payload_output_Subnets_Ipv6CidrBlockAssociationSet = glueContext.write_dynamic_frame.from_options(frame = network_data_root_payload_output_Subnets_Ipv6CidrBlockAssociationSet, connection_type = "s3", connection_options = {"path": "s3://rohit-glue-bucket/network_data_root_payload_output_Subnets_Ipv6CidrBlockAssociationSet/"}, format = "orc", transformation_ctx = "network_data_root_payload_output_Subnets_Ipv6CidrBlockAssociationSet")

network_data_root_NatGateways = glueContext.write_dynamic_frame.from_options(frame = network_data_root_NatGateways, connection_type = "s3", connection_options = {"path": "s3://rohit-glue-bucket/network_data_root_NatGateways/"}, format = "orc", transformation_ctx = "network_data_root_NatGateways")

network_data_root_payload_output_NatGateways = glueContext.write_dynamic_frame.from_options(frame = network_data_root_payload_output_NatGateways, connection_type = "s3", connection_options = {"path": "s3://rohit-glue-bucket/network_data_root_payload_output_NatGateways/"}, format = "orc", transformation_ctx = "network_data_root_payload_output_NatGateways")


