import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "toronto_school_db", table_name = "torontochoollocation", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "toronto_school_db", table_name = "torontochoollocation", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("_id", "long", "_id", "long"), ("objectid", "long", "objectid", "long"), ("geo_id", "long", "geo_id", "long"), ("name", "string", "name", "string"), ("school_level", "string", "school_level", "string"), ("school_type", "string", "school_type", "string"), ("board_name", "string", "board_name", "string"), ("source_address", "string", "source_address", "string"), ("school_type_desc", "string", "school_type_desc", "string"), ("address_point_id", "long", "address_point_id", "long"), ("address_number", "string", "address_number", "string"), ("linear_name_full", "string", "linear_name_full", "string"), ("address_full", "string", "address_full", "string"), ("postal_code", "string", "postal_code", "string"), ("municipality", "string", "municipality", "string"), ("city", "string", "city", "string"), ("place_name", "string", "place_name", "string"), ("general_use_code", "long", "general_use_code", "long"), ("centreline_id", "long", "centreline_id", "long"), ("lo_num", "long", "lo_num", "long"), ("lo_num_suf", "string", "lo_num_suf", "string"), ("hi_num", "double", "hi_num", "double"), ("hi_num_suf", "string", "hi_num_suf", "string"), ("linear_name_id", "long", "linear_name_id", "long"), ("x", "double", "x", "double"), ("y", "double", "y", "double"), ("latitude", "double", "latitude", "double"), ("longitude", "double", "longitude", "double"), ("geometry", "string", "geometry", "string"), ("partition_0", "string", "partition_0", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("_id", "long", "_id", "long"), ("objectid", "long", "objectid", "long"), ("geo_id", "long", "geo_id", "long"), ("name", "string", "name", "string"), ("school_level", "string", "school_level", "string"), ("school_type", "string", "school_type", "string"), ("board_name", "string", "board_name", "string"), ("source_address", "string", "source_address", "string"), ("school_type_desc", "string", "school_type_desc", "string"), ("address_point_id", "long", "address_point_id", "long"), ("address_number", "string", "address_number", "string"), ("linear_name_full", "string", "linear_name_full", "string"), ("address_full", "string", "address_full", "string"), ("postal_code", "string", "postal_code", "string"), ("municipality", "string", "municipality", "string"), ("city", "string", "city", "string"), ("place_name", "string", "place_name", "string"), ("general_use_code", "long", "general_use_code", "long"), ("centreline_id", "long", "centreline_id", "long"), ("lo_num", "long", "lo_num", "long"), ("lo_num_suf", "string", "lo_num_suf", "string"), ("hi_num", "double", "hi_num", "double"), ("hi_num_suf", "string", "hi_num_suf", "string"), ("linear_name_id", "long", "linear_name_id", "long"), ("x", "double", "x", "double"), ("y", "double", "y", "double"), ("latitude", "double", "latitude", "double"), ("longitude", "double", "longitude", "double"), ("geometry", "string", "geometry", "string"), ("partition_0", "string", "partition_0", "string")], transformation_ctx = "applymapping1")
## @type: SelectFields
## @args: [paths = ["_id", "objectid", "geo_id", "name", "school_level", "school_type", "board_name", "source_address", "school_type_desc", "address_point_id", "address_number", "linear_name_full", "address_full", "postal_code", "municipality", "city", "place_name", "general_use_code", "centreline_id", "lo_num", "lo_num_suf", "hi_num", "hi_num_suf", "linear_name_id", "x", "y", "latitude", "longitude", "geometry"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["_id", "objectid", "geo_id", "name", "school_level", "school_type", "board_name", "source_address", "school_type_desc", "address_point_id", "address_number", "linear_name_full", "address_full", "postal_code", "municipality", "city", "place_name", "general_use_code", "centreline_id", "lo_num", "lo_num_suf", "hi_num", "hi_num_suf", "linear_name_id", "x", "y", "latitude", "longitude", "geometry"], transformation_ctx = "selectfields2")
## @type: ResolveChoice
## @args: [choice = "MATCH_CATALOG", database = "toronto_school_db", table_name = "torontochoollocation", transformation_ctx = "resolvechoice3"]
## @return: resolvechoice3
## @inputs: [frame = selectfields2]
resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "toronto_school_db", table_name = "torontochoollocation", transformation_ctx = "resolvechoice3")
## @type: DataSink
## @args: [database = "toronto_school_db", table_name = "torontochoollocation", transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = resolvechoice3]
datasink4 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice3, database = "toronto_school_db", table_name = "torontochoollocation", transformation_ctx = "datasink4")
job.commit()