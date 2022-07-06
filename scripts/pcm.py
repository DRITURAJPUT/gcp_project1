import findspark
findspark.init()
from pyspark.sql.types import StructField, IntegerType,StringType,StructType
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging
import configparser

if __name__ == '__main__':

     spark = SparkSession.builder\
        .master("local[1]")\
        .appName("SparkByExamples.com")\
        .getOrCreate()



##import config path----------------------------------------------------------


     config=configparser.ConfigParser()
     config.read(r'../config/pcm_input.ini')
     inputfile =config.get('path','fileSchemapath')
     jsonfile2 = config.get('path', 'jsonSchemapath')
     spechaval = config.get('column','specharval').split(',')
     emailvali = config.get('column','emailvalid').split(',')
     mobvalid = config.get('column','mobileval' ).split(',')
     badatafi = config.get('path','badatafilepath')
     goodataf = config.get('path','goodatafilpath')
     logfile = config.get('path','logfilepath')


# create file record logger program------------------------------------------------------------------------------

     pcm_logger = logging.getLogger("pcm.log")
     pcm_logger.setLevel(logging.DEBUG)
     from utils.log_fun import logfun

     logpath = logfun(logfile)
     pcm = logging.FileHandler(logpath)
     pcm.setLevel(logging.DEBUG)
     pcm_formater = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
     pcm.setFormatter(pcm_formater)
     pcm_logger.addHandler(pcm)

##create dataframe------------------------------------------------------------

     from utils.schema_validation import dataframe

     df=dataframe(json_file=jsonfile2,input_path=inputfile)
     pcm_logger.info('dataframe created')

##spelcharval--------------------------------------------------------------------

     from utils.validation import spelcharval

     spcvaldf=spelcharval(df,spechaval)
     pcm_logger.info('special char validation')
##email-------------------------------------------------------------------------------

     from utils.validation import emailval

     emailvaldf = emailval(spcvaldf,emailvali)
     pcm_logger.info('email validation')

##mobil---------------------------------------------------------------------------

     from utils.validation import mob_num_val
     mobvaldf = mob_num_val(emailvaldf,mobvalid)
     mobvaldf.show()
     pcm_logger.info('mobile validation')

##bad data--------------------------------------------------------------------------+-----

     badf=df.join(mobvaldf,'dept_id','leftanti')
     badf.show()
     pcm_logger.info('bad data')

##bad data write----------------------------------------------------------------------------

     badf.write.mode('append').option('header',True).csv(badatafi)
     pcm_logger.info('bad data write')

##good data------------------------------------------------------------------------------------

     mobvaldf.write.mode('append').option('header',True).csv(goodataf)
     pcm_logger.info('good data write')