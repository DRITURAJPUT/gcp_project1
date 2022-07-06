import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,DateType,StringType
spark= SparkSession.builder.master('local[*]').appName('pcm_project').getOrCreate()

def dataframe(json_file,input_path):
    """

    :param json_file: # json schema path
    :return: # schema
    """
    schema=StructType()
    data_types={
        "integer":IntegerType(),
        "string":StringType(),
        "date":DateType(),
        "integer":IntegerType()
    }

    with open(json_file,'r')as jf:
        json_schema = jf.read()
        temp_schema=json_schema.strip('[]').replace('\n','').replace(' ','').replace('},{','}},{{').split('},{')
        schema_rdd1=spark.sparkContext.parallelize(temp_schema)
        schema_rdd=schema_rdd1.map(lambda x:json.loads(x)).collect()
        for i in schema_rdd:
            schema.add(i.get("name"),data_types.get(i.get('type')))



    df=spark.read.csv(input_path,schema=schema,header=True)
    return df



