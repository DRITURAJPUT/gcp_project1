from pyspark.sql.functions import col, udf, regexp_replace
from pyspark.sql.types import StringType


def spelcharval(df,column):
    """

    :param df: pass dataframe
    :param column: pass column name for validation
    :return: validated dataframe
    """

    for i in column:

       df=df.filter(col(i).rlike(r"^[a-zA-Z0-9]*$"))
    return df

##emailval-------------------------------------------------
from pyspark.sql.functions import col

import re

def emailval(df,column):
    """

    :param df: pass dataframe
    :param column:pass column for validation
    :return: validated dataframe
    """
    for i in column:
        df=df.filter(col(i).rlike(r'^\w+([\.-]?\w+)@\w+([\.-]?\w+)(\.\w{2,3})+$'))
        return df

##mobileva-------------------------------------------------------------------
def mob_num_val(df,col_nam):
   '''

    :param df: pass dataframe
    :param col_nam: pass column name for validation
    :return: validated dataframe
   '''
   if col_nam!=['']:
           for i in col_nam:

            def null(z): # check mob no len and 0 to 9 digit

                if len(z)==10 :
                   for i in z:
                    if i in ['0','1','2','3','4','5','6','7','8','9']:
                     return z

                else:
                    return ""



            spchar = udf(lambda z: null(z), StringType()) # udf function
            mobvdf = df.withColumn(i,regexp_replace(col(i),'null','null')).withColumn(i, spchar(col(i))).where(col(i) != '')

            return df
   else:
       return df


