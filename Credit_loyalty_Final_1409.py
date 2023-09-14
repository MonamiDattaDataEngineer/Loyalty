
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import date_sub
from pyspark.sql import functions as F
from datetime import datetime as dt
import argparse
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import when
from pyspark.sql.functions import *
from pyspark.sql.functions import col
from datetime import date, datetime, timedelta

  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", True)
spark.conf.set("spark.sql.adaptive.enabled",True)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled",True)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled",True)
spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
spark.conf.set("spark.sql.broadcastTimeout","6000")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead","CORRECTED")


from pyspark.sql.functions import col
from pyspark.sql.functions import date_sub
from pyspark.sql.functions import current_timestamp

df_AM_DEALER_LOC = spark.read.parquet("s3://msil-aos-raw/qlik-data/MULDMS.AM_DEALER_LOC/")
df_dmsd_ew_fin= spark.read.parquet("s3://msil-aos-raw/qlik-data/MULDMS.DMSD_EW_FIN/")
maindf = spark.read.parquet("s3://msil-aos-processed/LOYALTY/Credit_Query1/")

df_AM_DEALER_LOC.createOrReplaceTempView("AM_DEALER_LOC")
df_dmsd_ew_fin.createOrReplaceTempView("dmsd_ew_fin") 
maindf.createOrReplaceTempView("maindf")
##1st function lv_dlr_cd DATAFRAMES

df1 = spark.sql("""  select parent_group lv_parent_group,
           mul_dealer_cd ,
           for_cd,
           outlet_cd     
      from am_dealer_loc 
     """)

df2 = spark.sql ("""select 'MASS'||mul_dealer_cd lv_dlr_cd,
                    mul_dealer_cd dealer_cd,
                for_cd,
                outlet_cd
        from am_dealer_loc 
       where
          dealer_category='MAS'
         """)

df3 = spark.sql(""" 
         select dealer_cd,
                for_cd,
                outlet_cd,
         financier_delr_catg||financier_dlr_cd  lv_dlr_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'VCF' 
         """)           
df4 =spark.sql (""" 
         select dealer_cd,
                for_cd,
                outlet_cd,
         financier_delr_catg||financier_dlr_cd  lv_dlr_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'DDL' 
         """)
df5 =spark.sql (""" 
         select dealer_cd,
                for_cd,
                outlet_cd,
         financier_delr_catg||financier_dlr_cd  lv_dlr_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'DMM' 
         """)
df6 = spark.sql (""" 
         select dealer_cd,
                for_cd,
                outlet_cd,
         financier_delr_catg||financier_dlr_cd  lv_dlr_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'MUL' 
                     """)

df7 =spark.sql (""" 
         select dealer_cd,
                for_cd,
                
         financier_delr_catg||financier_dlr_cd  lv_dlr_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'VCF' 
         """)
df8 =spark.sql (""" 
         select dealer_cd,
                for_cd,
                
         financier_delr_catg||financier_dlr_cd  lv_dlr_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'DDL' 
         """)
df9 =spark.sql (""" 
         select dealer_cd,
                for_cd,
                
         financier_delr_catg||financier_dlr_cd  lv_dlr_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'DMM' 
         """)
df10 = spark.sql (""" 
         select dealer_cd,
                for_cd,
                
         financier_delr_catg||financier_dlr_cd  lv_dlr_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'MUL' 
         """)


df11 = spark.sql (""" 
         select dealer_cd,
               
                
         financier_delr_catg||financier_dlr_cd  lv_dlr_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'VCF' 
         """)
df12 = spark.sql (""" 
         select dealer_cd,
                
                
         financier_delr_catg||financier_dlr_cd  lv_dlr_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'DDL' 
         """)

df13 = spark.sql (""" 
         select dealer_cd,
                
                
         financier_delr_catg||financier_dlr_cd  lv_dlr_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'DMM' 
         """)
df14 = spark.sql (""" 
         select dealer_cd,
                
                
         financier_delr_catg||financier_dlr_cd  lv_dlr_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'MUL' 
         """)

#if lv_dlr_cd is null and lv_parent_group is not null then
df1.createOrReplaceTempView("df1")
df2.createOrReplaceTempView("df2")
df3.createOrReplaceTempView("df3")
df4.createOrReplaceTempView("df4")
df5.createOrReplaceTempView("df5")
df6.createOrReplaceTempView("df6")
df7.createOrReplaceTempView("df7")
df8.createOrReplaceTempView("df8")
df9.createOrReplaceTempView("df9")
df10.createOrReplaceTempView("df10")
df11.createOrReplaceTempView("df11")
df12.createOrReplaceTempView("df12")
df13.createOrReplaceTempView("df13")
df14.createOrReplaceTempView("df14")



df15 =spark.sql("""
      select ew.financier_delr_catg||ew.financier_dlr_cd   lv_dlr_cd,
                am.for_cd
      
       FROM am_dealer_loc am
         JOIN dmsd_ew_fin ew 
         ON ew.Dealer_cd=am.mul_dealer_cd
         and ew.for_Cd = am.for_cd
         and ew.outlet_Cd=am.outlet_cd
         
    JOIN df1 ON  am.parent_group=df1.lv_parent_group
   
         WHERE ason_date = DATE_SUB(CURRENT_DATE(), 1)
         and ew.financier_delr_catg = 'VCF'
      """)
df16 =spark.sql("""
      select ew.financier_delr_catg||ew.financier_dlr_cd   lv_dlr_cd,
                am.for_cd
      
       FROM am_dealer_loc am
         JOIN dmsd_ew_fin ew 
         ON ew.Dealer_cd=am.mul_dealer_cd
         and ew.for_Cd = am.for_cd
         and ew.outlet_Cd=am.outlet_cd
         
    JOIN df1 ON  am.parent_group=df1.lv_parent_group
   
         WHERE ason_date = DATE_SUB(CURRENT_DATE(), 1)
         and ew.financier_delr_catg = 'DDL'
      """)

df17 = spark.sql("""
      select ew.financier_delr_catg||ew.financier_dlr_cd   lv_dlr_cd,
                am.for_cd
      
       FROM am_dealer_loc am
         JOIN dmsd_ew_fin ew 
         ON ew.Dealer_cd=am.mul_dealer_cd
         and ew.for_Cd = am.for_cd
         and ew.outlet_Cd=am.outlet_cd
         
    JOIN df1 ON  am.parent_group=df1.lv_parent_group
   
         WHERE ason_date = DATE_SUB(CURRENT_DATE(), 1)
         and ew.financier_delr_catg = 'DMM'
      """)
df18 = spark.sql("""
      select ew.financier_delr_catg||ew.financier_dlr_cd   lv_dlr_cd,
                am.for_cd
      
       FROM am_dealer_loc am
         JOIN dmsd_ew_fin ew 
         ON ew.Dealer_cd=am.mul_dealer_cd
         and ew.for_Cd = am.for_cd
         and ew.outlet_Cd=am.outlet_cd
         
    JOIN df1 ON  am.parent_group=df1.lv_parent_group
   
         WHERE ason_date = DATE_SUB(CURRENT_DATE(), 1)
         and ew.financier_delr_catg = 'MUL'
      """)
df19 = spark.sql("""
      select ew.financier_delr_catg||ew.financier_dlr_cd   lv_dlr_cd,
              am.mul_dealer_cd dealer_cd,
              am.for_cd,
              am.outlet_cd
              
      
       FROM am_dealer_loc am
         JOIN dmsd_ew_fin ew 
         ON ew.Dealer_cd=am.mul_dealer_cd
         and ew.for_Cd = am.for_cd
         and ew.outlet_Cd=am.outlet_cd
         
    JOIN df1 ON  am.parent_group=df1.lv_parent_group
             AND am.mul_dealer_cd=df1.mul_dealer_cd
             AND am.for_cd=df1.for_cd
             AND am.outlet_cd=df1.outlet_cd
   
         WHERE ason_date = DATE_SUB(CURRENT_DATE(), 1)
         and ew.financier_delr_catg = 'VCF'
      """)

df20 =spark.sql("""
      select ew.financier_delr_catg||ew.financier_dlr_cd   lv_dlr_cd,
             am.mul_dealer_cd dealer_cd,
              am.for_cd,
              am.outlet_cd
                
      
       FROM am_dealer_loc am
         JOIN dmsd_ew_fin ew 
         ON ew.Dealer_cd=am.mul_dealer_cd
         and ew.for_Cd = am.for_cd
         and ew.outlet_Cd=am.outlet_cd
         
    JOIN df1 ON  am.parent_group=df1.lv_parent_group
             AND am.mul_dealer_cd=df1.mul_dealer_cd
             AND am.for_cd=df1.for_cd
             AND am.outlet_cd=df1.outlet_cd
   
         WHERE ason_date = DATE_SUB(CURRENT_DATE(), 1)
         and ew.financier_delr_catg = 'DDL'
      """)

df21 = spark.sql("""
      select ew.financier_delr_catg||ew.financier_dlr_cd   lv_dlr_cd,
             am.mul_dealer_cd dealer_cd,
              am.for_cd,
              am.outlet_cd
                
      
       FROM am_dealer_loc am
         JOIN dmsd_ew_fin ew 
         ON ew.Dealer_cd=am.mul_dealer_cd
         and ew.for_Cd = am.for_cd
         and ew.outlet_Cd=am.outlet_cd
         
   JOIN df1 ON  am.parent_group=df1.lv_parent_group
             AND am.mul_dealer_cd=df1.mul_dealer_cd
             AND am.for_cd=df1.for_cd
             AND am.outlet_cd=df1.outlet_cd
   
         WHERE ason_date = DATE_SUB(CURRENT_DATE(), 1)
         and ew.financier_delr_catg = 'DMM'
      """)

df22 = spark.sql("""
      select ew.financier_delr_catg||ew.financier_dlr_cd   lv_dlr_cd,
             am.mul_dealer_cd dealer_cd,
              am.for_cd,
              am.outlet_cd
                
      
       FROM am_dealer_loc am
         JOIN dmsd_ew_fin ew 
         ON ew.Dealer_cd=am.mul_dealer_cd
         and ew.for_Cd = am.for_cd
         and ew.outlet_Cd=am.outlet_cd
         
    JOIN df1 ON  am.parent_group=df1.lv_parent_group
             AND am.mul_dealer_cd=df1.mul_dealer_cd
             AND am.for_cd=df1.for_cd
             AND am.outlet_cd=df1.outlet_cd
   
         WHERE ason_date = DATE_SUB(CURRENT_DATE(), 1)
         and ew.financier_delr_catg = 'MUL'
      """)

df15.createOrReplaceTempView("df15")
df16.createOrReplaceTempView("df16")
df17.createOrReplaceTempView("df17")
df18.createOrReplaceTempView("df18")
df19.createOrReplaceTempView("df19")
df20.createOrReplaceTempView("df20")
df21.createOrReplaceTempView("df21")
df22.createOrReplaceTempView("df22")
#DFs functions FOR_CD



ddf1 = spark.sql("""  select parent_group lv_parent_group,
           mul_dealer_cd ,
           for_cd,
           outlet_cd     
      from am_dealer_loc 
     """)

ddf2 = spark.sql ("""select for_cd lv_for_cd,
                    mul_dealer_cd dealer_cd,
                for_cd,
                outlet_cd
        from am_dealer_loc 
       where
          dealer_category='MAS'
         """)

ddf3 = spark.sql(""" 
         select dealer_cd,
                for_cd,
                outlet_cd,
         financier_for_cd  lv_for_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'VCF' 
         """)           
ddf4 =spark.sql(""" 
         select dealer_cd,
                for_cd,
                outlet_cd,
         financier_for_cd  lv_for_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'DDL' 
         """)
ddf5 =spark.sql(""" 
         select dealer_cd,
                for_cd,
                outlet_cd,
         financier_for_cd  lv_for_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'DMM' 
         """)
ddf6 = spark.sql(""" 
         select dealer_cd,
                for_cd,
                outlet_cd,
         financier_for_cd  lv_for_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'MUL' 
         """)

ddf7 =spark.sql (""" 
         select dealer_cd,
                for_cd,
                
         financier_for_cd  lv_for_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'VCF' 
         """)
ddf8 =spark.sql (""" 
         select dealer_cd,
                for_cd,
                
         financier_for_cd  lv_for_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'DDL' 
         """)
ddf9 =spark.sql (""" 
         select dealer_cd,
                for_cd,
                
         financier_for_cd  lv_for_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'DMM' 
         """)
ddf10 = spark.sql (""" 
         select dealer_cd,
                for_cd,
                
         financier_for_cd  lv_for_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'MUL' 
         """)

ddf11 = spark.sql (""" 
         select dealer_cd,
               financier_for_cd  lv_for_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'VCF' 
         """)
ddf12 = spark.sql (""" 
         select dealer_cd,
               financier_for_cd  lv_for_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'DDL' 
         """)

ddf13 = spark.sql (""" 
         select dealer_cd,
               financier_for_cd  lv_for_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'DMM' 
         """)
ddf14 = spark.sql (""" 
         select dealer_cd,
               financier_for_cd  lv_for_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'MUL' 
         """)

#if lv_dlr_cd is null and lv_parent_group is not null then
ddf1.createOrReplaceTempView("ddf1")
ddf2.createOrReplaceTempView("ddf2")
ddf3.createOrReplaceTempView("ddf3")

ddf15 =spark.sql("""
      select financier_for_cd  lv_for_cd,
                am.for_cd
      
       FROM am_dealer_loc am
         JOIN dmsd_ew_fin ew 
         ON ew.Dealer_cd=am.mul_dealer_cd
         and ew.for_Cd = am.for_cd
         and ew.outlet_Cd=am.outlet_cd
         
    JOIN ddf1 ON  am.parent_group=ddf1.lv_parent_group
             AND am.mul_dealer_cd=ddf1.mul_dealer_cd
             AND am.for_cd=ddf1.for_cd
             AND am.outlet_cd=ddf1.outlet_cd
   
         WHERE ason_date = DATE_SUB(CURRENT_DATE(), 1)
         and ew.financier_delr_catg = 'VCF'
      """)
ddf16 =spark.sql("""
      select financier_for_cd  lv_for_cd,
                am.for_cd
      
       FROM am_dealer_loc am
         JOIN dmsd_ew_fin ew 
         ON ew.Dealer_cd=am.mul_dealer_cd
         and ew.for_Cd = am.for_cd
         and ew.outlet_Cd=am.outlet_cd
         
    JOIN ddf1 ON  am.parent_group=ddf1.lv_parent_group
             AND am.mul_dealer_cd=ddf1.mul_dealer_cd
             AND am.for_cd=ddf1.for_cd
             AND am.outlet_cd=ddf1.outlet_cd
   
         WHERE ason_date = DATE_SUB(CURRENT_DATE(), 1)
         and ew.financier_delr_catg = 'DDL'
      """)

ddf17 = spark.sql("""
      select financier_for_cd  lv_for_cd,
                am.for_cd
      
       FROM am_dealer_loc am
         JOIN dmsd_ew_fin ew 
         ON ew.Dealer_cd=am.mul_dealer_cd
         and ew.for_Cd = am.for_cd
         and ew.outlet_Cd=am.outlet_cd
         
    JOIN ddf1 ON  am.parent_group=ddf1.lv_parent_group
             AND am.mul_dealer_cd=ddf1.mul_dealer_cd
             AND am.for_cd=ddf1.for_cd
             AND am.outlet_cd=ddf1.outlet_cd
   
         WHERE ason_date = DATE_SUB(CURRENT_DATE(), 1)
         and ew.financier_delr_catg = 'DMM'
      """)
ddf18 = spark.sql("""
      select financier_for_cd  lv_for_cd,
                am.for_cd
      
       FROM am_dealer_loc am
         JOIN dmsd_ew_fin ew 
         ON ew.Dealer_cd=am.mul_dealer_cd
         and ew.for_Cd = am.for_cd
         and ew.outlet_Cd=am.outlet_cd
         
    JOIN ddf1 ON  am.parent_group=ddf1.lv_parent_group
             AND am.mul_dealer_cd=ddf1.mul_dealer_cd
             AND am.for_cd=ddf1.for_cd
             AND am.outlet_cd=ddf1.outlet_cd
   
         WHERE ason_date = DATE_SUB(CURRENT_DATE(), 1)
         and ew.financier_delr_catg = 'MUL'
      """)
ddf19 = spark.sql("""
      select financier_for_cd  lv_for_cd,
              am.mul_dealer_cd dealer_cd,
              am.for_cd,
              am.outlet_cd
              
      
       FROM am_dealer_loc am
         JOIN dmsd_ew_fin ew 
         ON ew.Dealer_cd=am.mul_dealer_cd
         and ew.for_Cd = am.for_cd
         and ew.outlet_Cd=am.outlet_cd
         
    JOIN ddf1 ON  am.parent_group=ddf1.lv_parent_group
             AND am.mul_dealer_cd=ddf1.mul_dealer_cd
             AND am.for_cd=ddf1.for_cd
             AND am.outlet_cd=ddf1.outlet_cd
   
         WHERE ason_date = DATE_SUB(CURRENT_DATE(), 1)
         and ew.financier_delr_catg = 'VCF'
      """)

ddf20 =spark.sql("""
      select financier_for_cd  lv_for_cd,
              am.mul_dealer_cd dealer_cd,
              am.for_cd,
              am.outlet_cd
              
      
       FROM am_dealer_loc am
         JOIN dmsd_ew_fin ew 
         ON ew.Dealer_cd=am.mul_dealer_cd
         and ew.for_Cd = am.for_cd
         and ew.outlet_Cd=am.outlet_cd
         
    JOIN ddf1 ON  am.parent_group=ddf1.lv_parent_group
             AND am.mul_dealer_cd=ddf1.mul_dealer_cd
             AND am.for_cd=ddf1.for_cd
             AND am.outlet_cd=ddf1.outlet_cd
   
         WHERE ason_date = DATE_SUB(CURRENT_DATE(), 1)
         and ew.financier_delr_catg = 'DDL'
      """)

ddf21 = spark.sql("""
      select financier_for_cd  lv_for_cd,
              am.mul_dealer_cd dealer_cd,
              am.for_cd,
              am.outlet_cd
              
      
       FROM am_dealer_loc am
         JOIN dmsd_ew_fin ew 
         ON ew.Dealer_cd=am.mul_dealer_cd
         and ew.for_Cd = am.for_cd
         and ew.outlet_Cd=am.outlet_cd
         
    JOIN ddf1 ON  am.parent_group=ddf1.lv_parent_group
             AND am.mul_dealer_cd=ddf1.mul_dealer_cd
             AND am.for_cd=ddf1.for_cd
             AND am.outlet_cd=ddf1.outlet_cd
   
         WHERE ason_date = DATE_SUB(CURRENT_DATE(), 1)
         and ew.financier_delr_catg = 'DMM'
      """)

ddf22 = spark.sql("""
      select financier_for_cd  lv_for_cd,
              am.mul_dealer_cd dealer_cd,
              am.for_cd,
              am.outlet_cd
              
      
       FROM am_dealer_loc am
         JOIN dmsd_ew_fin ew 
         ON ew.Dealer_cd=am.mul_dealer_cd
         and ew.for_Cd = am.for_cd
         and ew.outlet_Cd=am.outlet_cd
         
    JOIN ddf1 ON  am.parent_group=ddf1.lv_parent_group
             AND am.mul_dealer_cd=ddf1.mul_dealer_cd
             AND am.for_cd=ddf1.for_cd
             AND am.outlet_cd=ddf1.outlet_cd
   
         WHERE ason_date = DATE_SUB(CURRENT_DATE(), 1)
         and ew.financier_delr_catg = 'MUL'
      """)

#TempView for 2nd functions

ddf4.createOrReplaceTempView("ddf4")
ddf5.createOrReplaceTempView("ddf5")
ddf6.createOrReplaceTempView("ddf6")
ddf7.createOrReplaceTempView("ddf7")
ddf8.createOrReplaceTempView("ddf8")
ddf9.createOrReplaceTempView("ddf9")
ddf10.createOrReplaceTempView("ddf10")
ddf11.createOrReplaceTempView("ddf11")
ddf12.createOrReplaceTempView("ddf12")
ddf13.createOrReplaceTempView("ddf13")
ddf14.createOrReplaceTempView("ddf14")
ddf15.createOrReplaceTempView("ddf15")
ddf16.createOrReplaceTempView("ddf16")
ddf17.createOrReplaceTempView("ddf17")
ddf18.createOrReplaceTempView("ddf18")
ddf19.createOrReplaceTempView("ddf19")
ddf20.createOrReplaceTempView("ddf20")
ddf21.createOrReplaceTempView("ddf21")
ddf22.createOrReplaceTempView("ddf22")
#DATAFRAMES FUNCTIONS OUTLET_CD

#OUTLET_CD function

ddff1 = spark.sql("""  select parent_group lv_parent_group,
           mul_dealer_cd ,
           for_cd,
           outlet_cd     
      from am_dealer_loc 
     """)

ddff2 = spark.sql ("""select outlet_cd lv_outlet_cd,
                    mul_dealer_cd dealer_cd,
                 for_cd,
                outlet_cd
        from am_dealer_loc 
       where
          dealer_category='MAS'
         """)

ddff3 = spark.sql(""" 
         select financier_outlet_cd  lv_outlet_cd,
		       dealer_cd,
                for_cd,
                outlet_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'VCF' 
         """)           
ddff4 =spark.sql(""" 
         select financier_outlet_cd  lv_outlet_cd,
		       dealer_cd,
                for_cd,
                outlet_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'DDL' 
         """)
ddff5 =spark.sql(""" 
         select financier_outlet_cd  lv_outlet_cd,
		       dealer_cd,
                for_cd,
                outlet_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'DMM' 
         """)
ddff6 = spark.sql(""" 
         select financier_outlet_cd  lv_outlet_cd,
		       dealer_cd,
                for_cd,
                outlet_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'MUL' 
         """)

ddff7 =spark.sql (""" 
         select dealer_cd,
                for_cd,
                
         financier_outlet_cd  lv_outlet_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'VCF' 
         """)
ddff8 =spark.sql (""" 
         select dealer_cd,
                for_cd,
                
         financier_outlet_cd  lv_outlet_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'DDL' 
         """)
ddff9 =spark.sql (""" 
         select dealer_cd,
                for_cd,
                
         financier_outlet_cd  lv_outlet_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'DMM' 
         """)
ddff10 = spark.sql (""" 
         select dealer_cd,
                for_cd,
                
         financier_outlet_cd  lv_outlet_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'MUL' 
         """)
ddff11 = spark.sql (""" 
         select dealer_cd,
               financier_outlet_cd  lv_outlet_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'VCF' 
         """)
ddff12 = spark.sql (""" 
         select dealer_cd,
               financier_outlet_cd  lv_outlet_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'DDL' 
         """)

ddff13 = spark.sql (""" 
         select dealer_cd,
               financier_outlet_cd  lv_outlet_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'DMM' 
         """)
ddff14 = spark.sql (""" 
         select dealer_cd,
               financier_outlet_cd  lv_outlet_cd
        from dmsd_ew_fin 
        WHERE   
        ason_date = DATE_SUB(CURRENT_DATE(), 1)
        
         AND financier_delr_catg = 'MUL' 
         """)

#if lv_dlr_cd is null and lv_parent_group is not null then
ddff1.createOrReplaceTempView("ddff1")
ddff2.createOrReplaceTempView("ddff2")
ddff3.createOrReplaceTempView("ddff3")

ddff15 =spark.sql("""
      select ew.financier_outlet_cd  lv_outlet_cd,
                am.for_cd
      
       FROM am_dealer_loc am
         JOIN dmsd_ew_fin ew 
         ON ew.Dealer_cd=am.mul_dealer_cd
         and ew.for_Cd = am.for_cd
         and ew.outlet_Cd=am.outlet_cd
         
    JOIN ddff1 ON  am.parent_group=ddff1.lv_parent_group
             AND am.mul_dealer_cd=ddff1.mul_dealer_cd
             AND am.for_cd=ddff1.for_cd
             AND am.outlet_cd=ddff1.outlet_cd
   
         WHERE ason_date = DATE_SUB(CURRENT_DATE(), 1)
         and ew.financier_delr_catg = 'VCF'
      """)
ddff16 =spark.sql("""
      select ew.financier_outlet_cd  lv_outlet_cd,
                am.for_cd
      
       FROM am_dealer_loc am
         JOIN dmsd_ew_fin ew 
         ON ew.Dealer_cd=am.mul_dealer_cd
         and ew.for_Cd = am.for_cd
         and ew.outlet_Cd=am.outlet_cd
         
    JOIN ddff1 ON  am.parent_group=ddff1.lv_parent_group
             AND am.mul_dealer_cd=ddff1.mul_dealer_cd
             AND am.for_cd=ddff1.for_cd
             AND am.outlet_cd=ddff1.outlet_cd
   
         WHERE ason_date = DATE_SUB(CURRENT_DATE(), 1)
         and ew.financier_delr_catg = 'DDL'
      """)
ddff17 = spark.sql("""
      select ew.financier_outlet_cd  lv_outlet_cd,
                am.for_cd
      
       FROM am_dealer_loc am
         JOIN dmsd_ew_fin ew 
         ON ew.Dealer_cd=am.mul_dealer_cd
         and ew.for_Cd = am.for_cd
         and ew.outlet_Cd=am.outlet_cd
         
    JOIN ddff1 ON  am.parent_group=ddff1.lv_parent_group
             AND am.mul_dealer_cd=ddff1.mul_dealer_cd
             AND am.for_cd=ddff1.for_cd
             AND am.outlet_cd=ddff1.outlet_cd
   
         WHERE ason_date = DATE_SUB(CURRENT_DATE(), 1)
         and ew.financier_delr_catg = 'DMM'
      """)
ddff18 = spark.sql("""
      select ew.financier_outlet_cd  lv_outlet_cd,
                am.for_cd
      
       FROM am_dealer_loc am
         JOIN dmsd_ew_fin ew 
         ON ew.Dealer_cd=am.mul_dealer_cd
         and ew.for_Cd = am.for_cd
         and ew.outlet_Cd=am.outlet_cd
         
    JOIN ddff1 ON  am.parent_group=ddff1.lv_parent_group
             AND am.mul_dealer_cd=ddff1.mul_dealer_cd
             AND am.for_cd=ddff1.for_cd
             AND am.outlet_cd=ddff1.outlet_cd
   
         WHERE ason_date = DATE_SUB(CURRENT_DATE(), 1)
         and ew.financier_delr_catg = 'MUL'
      """)
ddff19 = spark.sql("""
      select financier_outlet_cd  lv_outlet_cd,
              am.mul_dealer_cd dealer_cd,
              am.for_cd,
              am.outlet_cd
              
      
       FROM am_dealer_loc am
         JOIN dmsd_ew_fin ew 
         ON ew.Dealer_cd=am.mul_dealer_cd
         and ew.for_Cd = am.for_cd
         and ew.outlet_Cd=am.outlet_cd
         
    JOIN ddff1 ON  am.parent_group=ddff1.lv_parent_group
             AND am.mul_dealer_cd=ddff1.mul_dealer_cd
             AND am.for_cd=ddff1.for_cd
             AND am.outlet_cd=ddff1.outlet_cd
   
         WHERE ason_date = DATE_SUB(CURRENT_DATE(), 1)
         and ew.financier_delr_catg = 'VCF'
      """)

ddff20 =spark.sql("""
      select financier_outlet_cd  lv_outlet_cd,
              am.mul_dealer_cd dealer_cd,
              am.for_cd,
              am.outlet_cd
              
      
       FROM am_dealer_loc am
         JOIN dmsd_ew_fin ew 
         ON ew.Dealer_cd=am.mul_dealer_cd
         and ew.for_Cd = am.for_cd
         and ew.outlet_Cd=am.outlet_cd
         
    JOIN ddff1 ON  am.parent_group=ddff1.lv_parent_group
             AND am.mul_dealer_cd=ddff1.mul_dealer_cd
             AND am.for_cd=ddff1.for_cd
             AND am.outlet_cd=ddff1.outlet_cd
   
         WHERE ason_date = DATE_SUB(CURRENT_DATE(), 1)
         and ew.financier_delr_catg = 'DDL'
      """)

ddff21 = spark.sql("""
      select financier_outlet_cd  lv_outlet_cd,
              am.mul_dealer_cd dealer_cd,
              am.for_cd,
              am.outlet_cd
              
      
       FROM am_dealer_loc am
         JOIN dmsd_ew_fin ew 
         ON ew.Dealer_cd=am.mul_dealer_cd
         and ew.for_Cd = am.for_cd
         and ew.outlet_Cd=am.outlet_cd
         
    JOIN ddff1 ON  am.parent_group=ddff1.lv_parent_group
             AND am.mul_dealer_cd=ddff1.mul_dealer_cd
             AND am.for_cd=ddff1.for_cd
             AND am.outlet_cd=ddff1.outlet_cd
   
         WHERE ason_date = DATE_SUB(CURRENT_DATE(), 1)
         and ew.financier_delr_catg = 'DMM'
      """)

ddff22 = spark.sql("""
      select financier_outlet_cd  lv_outlet_cd,
              am.mul_dealer_cd dealer_cd,
              am.for_cd,
              am.outlet_cd
              
      
       FROM am_dealer_loc am
         JOIN dmsd_ew_fin ew 
         ON ew.Dealer_cd=am.mul_dealer_cd
         and ew.for_Cd = am.for_cd
         and ew.outlet_Cd=am.outlet_cd
         
    JOIN ddff1 ON  am.parent_group=ddff1.lv_parent_group
             AND am.mul_dealer_cd=ddff1.mul_dealer_cd
             AND am.for_cd=ddff1.for_cd
             AND am.outlet_cd=ddff1.outlet_cd
   
         WHERE ason_date = DATE_SUB(CURRENT_DATE(), 1)
         and ew.financier_delr_catg = 'MUL'
      """)

#TempView for 3rd functions

ddff4.createOrReplaceTempView("ddff4")
ddff5.createOrReplaceTempView("ddff5")
ddff6.createOrReplaceTempView("ddff6")
ddff7.createOrReplaceTempView("ddff7")
ddff8.createOrReplaceTempView("ddff8")
ddff9.createOrReplaceTempView("ddff9")
ddff10.createOrReplaceTempView("ddff10")
ddff11.createOrReplaceTempView("ddff11")
ddff12.createOrReplaceTempView("ddff12")
ddff13.createOrReplaceTempView("ddff13")
ddff14.createOrReplaceTempView("ddff14")
ddff15.createOrReplaceTempView("ddff15")
ddff16.createOrReplaceTempView("ddff16")
ddff17.createOrReplaceTempView("ddff17")
ddff18.createOrReplaceTempView("ddff18")
ddff19.createOrReplaceTempView("ddff19")
ddff20.createOrReplaceTempView("ddff20")
ddff21.createOrReplaceTempView("ddff21")
ddff22.createOrReplaceTempView("ddff22")
###checking Trans_date for query_3

df_gd_loyalty_trans = spark.read.parquet("s3://msil-aos-raw/qlik-data/MULDMS.GD_LOYALTY_TRANS_TRANS_DATE/")

DF_gd_loyalty_enrol = spark.read.parquet("s3://msil-aos-raw/qlik-data/MULDMS.GD_LOYALTY_ENROL/")

DF_AM_LIST_RANGE = spark.read.parquet("s3://msil-aos-raw/qlik-data/MULDMS.AM_LIST_RANGE/")


df_gd_loyalty_trans.createOrReplaceTempView("gd_loyalty_trans")  
DF_gd_loyalty_enrol.createOrReplaceTempView("gd_loyalty_enrol") 
DF_AM_LIST_RANGE.createOrReplaceTempView("AM_LIST_RANGE")   

df_dealer_loyalty = df_AM_DEALER_LOC.join(df_gd_loyalty_trans,((col("dr_parent_group")== col("parent_group")) & 
                                                               (col("dr_dlr_map_cd")==col("dealer_map_cd")) &
                                                               (col("dr_loc_cd")== col("loc_cd")) & 
                                                               (col("trans_date")>= ("2023-06-20") ) & 
                                                               (col("trans_date") < ("2023-07-21")) &
                                                               (col("mul_dealer_cd") != "9967") ),"inner")
#df_dealer_loyalty = df_dealer_loyalty.filter(col("mul_dealer_cd") != "9967")
print(df_dealer_loyalty.count())
df_dealer_loyalty.createOrReplaceTempView("rec")


df_query3 = spark.sql("""
SELECT 
 
 'MUL AUTOCARD_LAD' AS source,

       am.mul_dealer_cd MUL_DEALER_CD,

       am.for_cd FOR_CD,
       am.outlet_cd OUTLET_CD,
        gd.trans_date old_trans_date,
       
     JOIN1.lv_parent_group lv_parent_group,JOIN2.lv_dlr_cd dlr2,JOIN3.lv_dlr_cd dlr3,JOIN4.lv_dlr_cd dlr4, JOIN5.lv_dlr_cd dlr5, JOIN6.lv_dlr_cd dlr6, JOIN7.lv_dlr_cd dlr7,JOIN8.lv_dlr_cd dlr8,
    JOIN9.lv_dlr_cd dlr9,JOIN10.lv_dlr_cd dlr10,JOIN11.lv_dlr_cd dlr11,JOIN12.lv_dlr_cd dlr12 ,JOIN13.lv_dlr_cd dlr13,JOIN14.lv_dlr_cd dlr14,
    JOIN15.lv_dlr_cd dlr15,JOIN16.lv_dlr_cd dlr16,JOIN17.lv_dlr_cd dlr17,JOIN18.lv_dlr_cd dlr18,JOIN19.lv_dlr_cd dlr19,JOIN20.lv_dlr_cd dlr20,
    JOIN21.lv_dlr_cd dlr21,JOIN22.lv_dlr_cd dlr22,
    
   JOINN1.lv_parent_group lv_parent_groupFOR,JOINN2.lv_for_cd for2,JOINN3.lv_for_cd for3,JOINN4.lv_for_cd for4,JOINN5.lv_for_cd for5,
       JOINN6.lv_for_cd for6,JOINN7.lv_for_cd for7,JOINN8.lv_for_cd for8,JOINN9.lv_for_cd for9,JOINN10.lv_for_cd for10,JOINN11.lv_for_cd for11,JOINN12.lv_for_cd for12,
       JOINN13.lv_for_cd for13,
       JOINN14.lv_for_cd for14,JOINN15.lv_for_cd for15,JOINN16.lv_for_cd for16,JOINN17.lv_for_cd for17,JOINN18.lv_for_cd for18,JOINN19.lv_for_cd for19,
       JOINN20.lv_for_cd for20,JOINN21.lv_for_cd for21,JOINN22.lv_for_cd for22,
       
       JJOINN1.lv_parent_group lv_parent_groupOUTLET,JJOINN2.lv_outlet_cd outlet2,JJOINN3.lv_outlet_cd outlet3,JJOINN4.lv_outlet_cd outlet4,JJOINN5.lv_outlet_cd outlet5,
       JJOINN6.lv_outlet_cd outlet6,JJOINN7.lv_outlet_cd outlet7,JJOINN8.lv_outlet_cd outlet8,JJOINN9.lv_outlet_cd outlet9,JJOINN10.lv_outlet_cd outlet10,JJOINN11.lv_outlet_cd outlet11,
       JJOINN12.lv_outlet_cd outlet12,JJOINN13.lv_outlet_cd outlet13,
       JJOINN14.lv_outlet_cd outlet14,JJOINN15.lv_outlet_cd outlet15,JJOINN16.lv_outlet_cd outlet16,JJOINN17.lv_outlet_cd outlet17,JJOINN18.lv_outlet_cd outlet18,JJOINN19.lv_outlet_cd outlet19,
       JJOINN20.lv_outlet_cd outlet20,JJOINN21.lv_outlet_cd outlet21,JJOINN22.lv_outlet_cd outlet22,
       
       
       
       ar.list_code,
    
     

       CURRENT_DATE AS TRANS_DATE,
       'INR' AS ARST_CURRENCY_CODE,
       

       

       'USER' AS ARST_CONVERSION_TYPE,

       '1' AS ARST_CONVERSION_RATE,
       
      gd.security_amount_debt,
      

       CASE am.channel
              WHEN 'COM'THEN 'PRAGATI CREDIT AGAINST POINTS REDEEMED IN '
             ELSE 'MARUTI SUZUKI REWARDS CREDIT AGAINST POINTS REDEEMED IN ' END
       || SUBSTRING(gd.trans_date, 4) AS ARST_REMARKS,



       'S' AS ARST_ACCOUNT_TYPE,

       'N' AS ARST_FUND_VALIDATION,


       'LINE' AS LINE_TYPE,

       '1' AS LINE_NUMBER,
       " " AS ARST_LINK_LINE,

  
        'R' AS ARST_PROCESS_FLAG,

       'MISC' AS ARST_PMOD_CODE,

       ar.list_code AS CARD_TYPE,

       am.channel,

       DATE_FORMAT(gd.trans_date, 'MMM') AS CREATION_MONTH,

       CASE DATE_FORMAT(gd.trans_date, 'MMM')
           WHEN 'JAN' THEN YEAR(gd.trans_date) - 1
           WHEN 'FEB' THEN YEAR(gd.trans_date) - 1
           WHEN 'MAR' THEN YEAR(gd.trans_date) - 1
           ELSE YEAR(gd.trans_date)
       END AS FIN_YEAR,

      gd.trans_date AS ARST_CREATION_DATE,

       " " AS TRANSFER_FLAG,

       am.parent_group,

       am.region_cd



       



FROM am_dealer_loc am

LEFT OUTER JOIN 
     df1 JOIN1
     ON am.mul_dealer_cd = JOIN1.mul_dealer_cd
     and am.for_cd = JOIN1.for_cd
     and am.outlet_cd = JOIN1.outlet_cd
     
LEFT OUTER JOIN 
     df2 JOIN2
     ON am.mul_dealer_cd = JOIN2.dealer_cd
     and am.for_cd = JOIN2.for_cd
     and am.outlet_cd = JOIN2.outlet_cd  
     
LEFT OUTER JOIN 
     df3 JOIN3
     ON am.mul_dealer_cd = JOIN3.dealer_cd
     and am.for_cd = JOIN3.for_cd
     and am.outlet_cd = JOIN3.outlet_cd 
     
LEFT OUTER JOIN 
     df4 JOIN4
     ON am.mul_dealer_cd = JOIN4.dealer_cd
     and am.for_cd = JOIN4.for_cd
     and am.outlet_cd = JOIN4.outlet_cd     
LEFT OUTER JOIN 
     df5 JOIN5
     ON am.mul_dealer_cd = JOIN5.dealer_cd
     and am.for_cd = JOIN5.for_cd
     and am.outlet_cd = JOIN5.outlet_cd 
     
LEFT OUTER JOIN 
     df6 JOIN6
     ON am.mul_dealer_cd = JOIN6.dealer_cd
     and am.for_cd = JOIN6.for_cd
     and am.outlet_cd = JOIN6.outlet_cd

LEFT OUTER JOIN 
     df7 JOIN7
     ON am.mul_dealer_cd = JOIN7.dealer_cd
     and am.for_cd = JOIN7.for_cd
LEFT OUTER JOIN 
     df8 JOIN8
     ON am.mul_dealer_cd = JOIN8.dealer_cd
     and am.for_cd = JOIN8.for_cd
LEFT OUTER JOIN 
     df9 JOIN9
     ON am.mul_dealer_cd = JOIN9.dealer_cd
     and am.for_cd = JOIN9.for_cd 
LEFT OUTER JOIN 
     df10 JOIN10
     ON am.mul_dealer_cd = JOIN10.dealer_cd
     and am.for_cd = JOIN10.for_cd  
     
LEFT OUTER JOIN 
     df11 JOIN11
     ON am.mul_dealer_cd = JOIN11.dealer_cd   
LEFT OUTER JOIN 
     df12 JOIN12
     ON am.mul_dealer_cd = JOIN12.dealer_cd  
LEFT OUTER JOIN 
     df13 JOIN13
     ON am.mul_dealer_cd = JOIN13.dealer_cd 
LEFT OUTER JOIN 
     df14 JOIN14
     ON am.mul_dealer_cd = JOIN14.dealer_cd
     
LEFT OUTER JOIN 
     df15 JOIN15
     ON am.for_cd = JOIN15.for_cd   
LEFT OUTER JOIN 
     df16 JOIN16
     ON am.for_cd = JOIN16.for_cd    
LEFT OUTER JOIN 
     df17 JOIN17
     ON am.for_cd = JOIN17.for_cd 
LEFT OUTER JOIN 
     df18 JOIN18
     ON am.for_cd = JOIN18.for_cd
     
LEFT OUTER JOIN 
     df19 JOIN19
     ON am.mul_dealer_cd = JOIN19.dealer_cd
     and am.for_cd = JOIN19.for_cd
     and am.outlet_cd = JOIN19.outlet_cd 
LEFT OUTER JOIN 
     df20 JOIN20
     ON am.mul_dealer_cd = JOIN20.dealer_cd
     and am.for_cd = JOIN20.for_cd
     and am.outlet_cd = JOIN20.outlet_cd   
LEFT OUTER JOIN 
     df21 JOIN21
     ON am.mul_dealer_cd = JOIN21.dealer_cd
     and am.for_cd = JOIN21.for_cd
     and am.outlet_cd = JOIN21.outlet_cd 
LEFT OUTER JOIN 
     df22 JOIN22
     ON am.mul_dealer_cd = JOIN22.dealer_cd
     and am.for_cd = JOIN22.for_cd
     and am.outlet_cd = JOIN22.outlet_cd 
     
     
LEFT OUTER JOIN 
     ddf1 JOINN1
     ON am.mul_dealer_cd = JOINN1.mul_dealer_cd
     and am.for_cd = JOINN1.for_cd
     and am.outlet_cd = JOINN1.outlet_cd
     
LEFT OUTER JOIN 
     ddf2 JOINN2
     ON am.mul_dealer_cd = JOINN2.dealer_cd
     and am.for_cd = JOINN2.lv_for_cd
     and am.outlet_cd = JOINN2.outlet_cd  
     
LEFT OUTER JOIN 
     ddf3 JOINN3
     ON am.mul_dealer_cd = JOINN3.dealer_cd
     and am.for_cd = JOINN3.for_cd
     and am.outlet_cd = JOINN3.outlet_cd 
     
LEFT OUTER JOIN 
     ddf4 JOINN4
     ON am.mul_dealer_cd = JOINN4.dealer_cd
     and am.for_cd = JOINN4.for_cd
     and am.outlet_cd = JOINN4.outlet_cd     
LEFT OUTER JOIN 
     ddf5 JOINN5
     ON am.mul_dealer_cd = JOINN5.dealer_cd
     and am.for_cd = JOINN5.for_cd
     and am.outlet_cd = JOINN5.outlet_cd 
     
LEFT OUTER JOIN 
     ddf6 JOINN6
     ON am.mul_dealer_cd = JOINN6.dealer_cd
     and am.for_cd = JOINN6.for_cd
     and am.outlet_cd = JOINN6.outlet_cd

LEFT OUTER JOIN 
     ddf7 JOINN7
     ON am.mul_dealer_cd = JOINN7.dealer_cd
     and am.for_cd = JOINN7.for_cd
LEFT OUTER JOIN 
     ddf8 JOINN8
     ON am.mul_dealer_cd = JOINN8.dealer_cd
     and am.for_cd = JOINN8.for_cd
LEFT OUTER JOIN 
     ddf9 JOINN9
     ON am.mul_dealer_cd = JOINN9.dealer_cd
     and am.for_cd = JOINN9.for_cd 
LEFT OUTER JOIN 
     ddf10 JOINN10
     ON am.mul_dealer_cd = JOINN10.dealer_cd
     and am.for_cd = JOINN10.for_cd  
     
LEFT OUTER JOIN 
     ddf11 JOINN11
     ON am.mul_dealer_cd = JOINN11.dealer_cd   
LEFT OUTER JOIN 
     ddf12 JOINN12
     ON am.mul_dealer_cd = JOINN12.dealer_cd  
LEFT OUTER JOIN 
     ddf13 JOINN13
     ON am.mul_dealer_cd = JOINN13.dealer_cd 
LEFT OUTER JOIN 
     ddf14 JOINN14
     ON am.mul_dealer_cd = JOINN14.dealer_cd
     
LEFT OUTER JOIN 
     ddf15 JOINN15
     ON am.for_cd = JOINN15.for_cd   
LEFT OUTER JOIN 
     ddf16 JOINN16
     ON am.for_cd = JOINN16.for_cd    
LEFT OUTER JOIN 
     ddf17 JOINN17
     ON am.for_cd = JOINN17.for_cd 
LEFT OUTER JOIN 
     ddf18 JOINN18
     ON am.for_cd = JOINN18.for_cd
     
LEFT OUTER JOIN 
     ddf19 JOINN19
     ON am.mul_dealer_cd = JOINN19.dealer_cd
     and am.for_cd = JOINN19.for_cd
     and am.outlet_cd = JOINN19.outlet_cd 
LEFT OUTER JOIN 
     ddf20 JOINN20
     ON am.mul_dealer_cd = JOINN20.dealer_cd
     and am.for_cd = JOINN20.for_cd
     and am.outlet_cd = JOINN20.outlet_cd   
LEFT OUTER JOIN 
     ddf21 JOINN21
     ON am.mul_dealer_cd = JOINN21.dealer_cd
     and am.for_cd = JOINN21.for_cd
     and am.outlet_cd = JOINN21.outlet_cd 
LEFT OUTER JOIN 
     ddf22 JOINN22
     ON am.mul_dealer_cd = JOINN22.dealer_cd
     and am.for_cd = JOINN22.for_cd
     and am.outlet_cd = JOINN22.outlet_cd      
     




LEFT OUTER JOIN 
     ddff1 JJOINN1
     ON am.mul_dealer_cd = JJOINN1.mul_dealer_cd
     and am.for_cd = JJOINN1.for_cd
     and am.outlet_cd = JJOINN1.outlet_cd
     
LEFT OUTER JOIN 
     ddff2 JJOINN2
     ON am.mul_dealer_cd = JJOINN2.dealer_cd
     and am.for_cd = JJOINN2.for_cd
     and am.outlet_cd = JJOINN2.outlet_cd  
     
LEFT OUTER JOIN 
     ddff3 JJOINN3
     ON am.mul_dealer_cd = JJOINN3.dealer_cd
     and am.for_cd = JJOINN3.for_cd
     and am.outlet_cd = JJOINN3.outlet_cd 
     
LEFT OUTER JOIN 
     ddff4 JJOINN4
     ON am.mul_dealer_cd = JJOINN4.dealer_cd
     and am.for_cd = JJOINN4.for_cd
     and am.outlet_cd = JJOINN4.outlet_cd     
LEFT OUTER JOIN 
     ddff5 JJOINN5
     ON am.mul_dealer_cd = JJOINN5.dealer_cd
     and am.for_cd = JJOINN5.for_cd
     and am.outlet_cd = JJOINN5.outlet_cd 
     
LEFT OUTER JOIN 
     ddff6 JJOINN6
     ON am.mul_dealer_cd = JJOINN6.dealer_cd
     and am.for_cd = JJOINN6.for_cd
     and am.outlet_cd = JJOINN6.outlet_cd

LEFT OUTER JOIN 
     ddff7 JJOINN7
     ON am.mul_dealer_cd = JJOINN7.dealer_cd
     and am.for_cd = JJOINN7.for_cd
LEFT OUTER JOIN 
     ddff8 JJOINN8
     ON am.mul_dealer_cd = JJOINN8.dealer_cd
     and am.for_cd = JJOINN8.for_cd
LEFT OUTER JOIN 
     ddff9 JJOINN9
     ON am.mul_dealer_cd = JJOINN9.dealer_cd
     and am.for_cd = JJOINN9.for_cd 
LEFT OUTER JOIN 
     ddff10 JJOINN10
     ON am.mul_dealer_cd = JJOINN10.dealer_cd
     and am.for_cd = JJOINN10.for_cd  
     
LEFT OUTER JOIN 
     ddff11 JJOINN11
     ON am.mul_dealer_cd = JJOINN11.dealer_cd   
LEFT OUTER JOIN 
     ddff12 JJOINN12
     ON am.mul_dealer_cd = JJOINN12.dealer_cd  
LEFT OUTER JOIN 
     ddff13 JJOINN13
     ON am.mul_dealer_cd = JJOINN13.dealer_cd 
LEFT OUTER JOIN 
     ddff14 JJOINN14
     ON am.mul_dealer_cd = JJOINN14.dealer_cd
     
LEFT OUTER JOIN 
     ddff15 JJOINN15
     ON am.for_cd = JJOINN15.for_cd   
LEFT OUTER JOIN 
     ddff16 JJOINN16
     ON am.for_cd = JJOINN16.for_cd    
LEFT OUTER JOIN 
     ddff17 JJOINN17
     ON am.for_cd = JJOINN17.for_cd 
LEFT OUTER JOIN 
     ddff18 JJOINN18
     ON am.for_cd = JJOINN18.for_cd
     
LEFT OUTER JOIN 
     ddff19 JJOINN19
     ON am.mul_dealer_cd = JJOINN19.dealer_cd
     and am.for_cd = JJOINN19.for_cd
     and am.outlet_cd = JJOINN19.outlet_cd 
LEFT OUTER JOIN 
     ddff20 JJOINN20
     ON am.mul_dealer_cd = JJOINN20.dealer_cd
     and am.for_cd = JJOINN20.for_cd
     and am.outlet_cd = JJOINN20.outlet_cd   
LEFT OUTER JOIN 
     ddff21 JJOINN21
     ON am.mul_dealer_cd = JJOINN21.dealer_cd
     and am.for_cd = JJOINN21.for_cd
     and am.outlet_cd = JJOINN21.outlet_cd 
LEFT OUTER JOIN 
     ddff22 JJOINN22
     ON am.mul_dealer_cd = JJOINN22.dealer_cd
     and am.for_cd = JJOINN22.for_cd
     and am.outlet_cd = JJOINN22.outlet_cd 

JOIN    
     rec
     on am.parent_group = rec.dr_parent_group
        AND am.dealer_map_cd = rec.dr_dlr_map_cd
        AND am.loc_cd = rec.dr_loc_cd

JOIN 
    gd_loyalty_trans gd  ON gd.dr_parent_group = am.parent_group 
                          AND gd.dr_dlr_map_cd = am.dealer_map_cd 
                         AND gd.dr_loc_cd = am.loc_cd
JOIN 
    gd_loyalty_enrol le ON le.card_num = gd.card_num
    
JOIN 
    AM_LIST_RANGE AR ON AR.principal_map_cd = am.principal_map_cd 
                    AND AR.list_name = 'CARD_TYPE' 
                    AND AR.list_code = le.card_type 
                    AND IFNULL(ar.list_flag, 'Y') = 'Y' 
                    AND AR.list_grp_code = le.channel
    
                         

WHERE gd.debit_pts IS NOT NULL
AND gd.trans_date >= '2023-06-21'
AND gd.trans_date < '2023-07-21'
---AND am.mul_dealer_cd != '9967'



          
 """)

#df_query3.show()






#####transdate###
###query4 with trans_date

#df_query3.show()

df_query3.createOrReplaceTempView("df_query3")

df_query4 = spark.sql("""
SELECT 
 
 source,

        MUL_DEALER_CD,
        FOR_CD,
        OUTLET_CD,
        old_trans_date,
        lv_parent_group,
        lv_parent_groupFOR,
        lv_parent_groupOUTLET,
        
        dlr15, dlr16, dlr17, dlr18, dlr19, dlr20,
     dlr21, dlr22,
       for15,for16,for17,for18,for19,for20,for21,for22,
       outlet15,outlet16,outlet17,outlet18,outlet19,outlet20,outlet21,outlet22,
       
     coalesce(dlr2, dlr3, dlr4,  dlr5,  dlr6, dlr7, dlr8,
     dlr9, dlr10,dlr11, dlr12 , dlr13, dlr14) as ARST_BILL_TO_DEALER_CODE1,
     
     coalesce(for2, for3, for4,  for5,  for6, for7, for8,
     for9, for10,for11, for12 , for13, for14) as ARST_BILL_TO_FOR_CODE1,
     
     coalesce(outlet2, outlet3, outlet4,  outlet5,  outlet6, outlet7, outlet8,
     outlet9, outlet10,outlet11, outlet12 , outlet13, outlet14) as ARST_BILL_TO_OUTLET_CODE1,
     
       list_code,
       TRANS_DATE,
       ARST_CURRENCY_CODE,
       TRANS_DATE,

       ARST_CURRENCY_CODE,

        ARST_CONVERSION_TYPE,

       ARST_CONVERSION_RATE,
       
      ROUND(SUM(security_amount_debt), 2) AS AMOUNT,
      

        ARST_REMARKS,
ARST_ACCOUNT_TYPE,
ARST_FUND_VALIDATION,
LINE_TYPE,
LINE_NUMBER,
ARST_LINK_LINE,
ARST_PROCESS_FLAG,
ARST_PMOD_CODE,
CARD_TYPE,
channel,
CREATION_MONTH,
FIN_YEAR,
ARST_CREATION_DATE,
TRANSFER_FLAG,
parent_group,
region_cd
       
       from df_query3
       
       GROUP BY
       SOURCE,
       MUL_DEALER_CD,
        FOR_CD, 
        OUTLET_CD,
        channel,
       CARD_TYPE,
          CREATION_MONTH,
          TRANS_DATE, 
          parent_group, 
          region_cd,
         dlr2, dlr3,dlr4,dlr5,dlr6,dlr7,dlr8,dlr9,
         dlr10,dlr11,dlr12,dlr13,dlr14,dlr15,dlr16,
         dlr17,
         dlr18,dlr19,dlr20,dlr21,dlr22,
         
         for2, for3, for4,  for5,  for6, for7, for8,
     for9, for10,for11, for12 , for13, for14,for15,for16,for17,for18,for19,for20,for21,for22,
     outlet2, outlet3, outlet4,  outlet5,  outlet6, outlet7, outlet8,
     outlet9, outlet10,outlet11, outlet12 , outlet13, outlet14,
     outlet15,outlet16,outlet17,outlet18,outlet19,outlet20,outlet21,outlet22,
         FIN_YEAR,
          ARST_REMARKS,
ARST_ACCOUNT_TYPE,
ARST_FUND_VALIDATION,
LINE_TYPE,
LINE_NUMBER,
ARST_LINK_LINE,
ARST_PROCESS_FLAG,
ARST_PMOD_CODE,
CARD_TYPE,
channel,
CREATION_MONTH,
FIN_YEAR,
ARST_CREATION_DATE,
TRANSFER_FLAG,
parent_group,
region_cd,
ARST_CURRENCY_CODE,
ARST_CONVERSION_TYPE,
ARST_CONVERSION_RATE,
lv_parent_group,
lv_parent_groupFOR,
lv_parent_groupOUTLET,
old_trans_date,
list_code
 """)


#df_query3.printSchema()
#df_query3.count()
#df_query4.show()
##query5 with transdate---

df_query4.createOrReplaceTempView("df_query4")
df_query5 = spark.sql("""
SELECT 
 
 source,

        MUL_DEALER_CD,
        FOR_CD,
        OUTLET_CD,
        old_trans_date,
        
       
     coalesce(ARST_BILL_TO_DEALER_CODE1,lv_parent_group,dlr15, dlr16, dlr17, dlr18, dlr19, dlr20,
     dlr21, dlr22) as ARST_BILL_TO_DEALER_CODE,
     
      coalesce(ARST_BILL_TO_FOR_CODE1,lv_parent_groupFOR, for15,for16,for17,for18,for19,for20,for21,for22) as ARST_BILL_TO_FOR_CODE,
      
      coalesce(ARST_BILL_TO_OUTLET_CODE1,lv_parent_groupOUTLET,outlet15,outlet16,outlet17,outlet18,outlet19,outlet20,outlet21,outlet22) as ARST_BILL_TO_OUTLET_CODE,
       list_code,
       TRANS_DATE,
       ARST_CURRENCY_CODE,
     

    

        ARST_CONVERSION_TYPE,
        ARST_CONVERSION_RATE,

      
       
       AMOUNT,
      

        ARST_REMARKS,
ARST_ACCOUNT_TYPE,
ARST_FUND_VALIDATION,
LINE_TYPE,
LINE_NUMBER,
ARST_LINK_LINE,
ARST_PROCESS_FLAG,
ARST_PMOD_CODE,
CARD_TYPE,
channel,
CREATION_MONTH,
FIN_YEAR,
ARST_CREATION_DATE,
TRANSFER_FLAG,
parent_group,
region_cd
       
       from df_query4
       
       
 """)


df_query5.printSchema()
#df_query3.count()
#df_query5.show()
##Adding columns---
df_query5 = df_query5.withColumn('ARST_CUSTOMER_CATEGORY', F.substring(col('ARST_BILL_TO_DEALER_CODE'), 1,3))


##Adding LV_batch number
df_query5 = df_query5.withColumn('financial_year', when(month('old_trans_date') < 4, year('old_trans_date')-1).otherwise(year('old_trans_date')))
df_query5= df_query5.withColumn('financial_month', when(month('old_trans_date') < 4, concat(lit('0'),month('old_trans_date') + 9)).otherwise(concat(lit('00'),month('TRANS_DATE') - 3)))
df_query5 = df_query5.withColumn('lv_batch_number', concat(lit('CBT'),col('financial_year'),col('financial_month')))

df_query5.printSchema()

#####Adding Column---TRANS_NUM

df_query5 = df_query5.withColumn('channel_mod', when(col('channel') == 'COM','C').when(col('channel') == 'EXC','N').when(col('channel') == 'NRM','A'))\
                    .withColumn('TRANS_NUM', concat(F.substring(col('lv_batch_number'),1,5),col('list_code'),col('channel_mod'),lit('-'),F.substring(col('ARST_BILL_TO_DEALER_CODE'),1,5),lit('-'),col('ARST_BILL_TO_OUTLET_CODE')))
##Adding Column ELEMENT

df_query5=  df_query5.withColumn('Element',when(col('ARST_CUSTOMER_CATEGORY')=='VCF','AC_RRPD').otherwise('AC_RRPSD'))
#query6 with trans_date

df_dealer_loyalty = df_AM_DEALER_LOC.join(df_gd_loyalty_trans,((col("dr_parent_group")== col("parent_group")) & 
                                                               (col("dr_dlr_map_cd")==col("dealer_map_cd")) &
                                                               (col("dr_loc_cd")== col("loc_cd")) & 
                                                               (col("trans_date")>= ("2020-06-20") ) & 
                                                               (col("trans_date") < ("2020-07-21")) &
                                                               (col("mul_dealer_cd") != "9967") ),"inner")
#df_dealer_loyalty = df_dealer_loyalty.filter(col("mul_dealer_cd") != "9967")
print(df_dealer_loyalty.count())
df_dealer_loyalty.createOrReplaceTempView("rec")

df_query6 = spark.sql("""
SELECT 
 
 'MUL AUTOCARD_LAD' AS source,

       am.mul_dealer_cd MUL_DEALER_CD,

       am.for_cd FOR_CD,
       am.outlet_cd OUTLET_CD,
       gd.trans_date old_trans_date,
       
     JOIN1.lv_parent_group lv_parent_group,JOIN2.lv_dlr_cd dlr2,JOIN3.lv_dlr_cd dlr3,JOIN4.lv_dlr_cd dlr4, JOIN5.lv_dlr_cd dlr5, JOIN6.lv_dlr_cd dlr6, JOIN7.lv_dlr_cd dlr7,JOIN8.lv_dlr_cd dlr8,
    JOIN9.lv_dlr_cd dlr9,JOIN10.lv_dlr_cd dlr10,JOIN11.lv_dlr_cd dlr11,JOIN12.lv_dlr_cd dlr12 ,JOIN13.lv_dlr_cd dlr13,JOIN14.lv_dlr_cd dlr14,
    JOIN15.lv_dlr_cd dlr15,JOIN16.lv_dlr_cd dlr16,JOIN17.lv_dlr_cd dlr17,JOIN18.lv_dlr_cd dlr18,JOIN19.lv_dlr_cd dlr19,JOIN20.lv_dlr_cd dlr20,
    JOIN21.lv_dlr_cd dlr21,JOIN22.lv_dlr_cd dlr22,
    
   JOINN1.lv_parent_group lv_parent_groupFOR,JOINN2.lv_for_cd for2,JOINN3.lv_for_cd for3,JOINN4.lv_for_cd for4,JOINN5.lv_for_cd for5,
       JOINN6.lv_for_cd for6,JOINN7.lv_for_cd for7,JOINN8.lv_for_cd for8,JOINN9.lv_for_cd for9,JOINN10.lv_for_cd for10,JOINN11.lv_for_cd for11,JOINN12.lv_for_cd for12,
       JOINN13.lv_for_cd for13,
       JOINN14.lv_for_cd for14,JOINN15.lv_for_cd for15,JOINN16.lv_for_cd for16,JOINN17.lv_for_cd for17,JOINN18.lv_for_cd for18,JOINN19.lv_for_cd for19,
       JOINN20.lv_for_cd for20,JOINN21.lv_for_cd for21,JOINN22.lv_for_cd for22,
       
       JJOINN1.lv_parent_group lv_parent_groupOUTLET,JJOINN2.lv_outlet_cd outlet2,JJOINN3.lv_outlet_cd outlet3,JJOINN4.lv_outlet_cd outlet4,JJOINN5.lv_outlet_cd outlet5,
       JJOINN6.lv_outlet_cd outlet6,JJOINN7.lv_outlet_cd outlet7,JJOINN8.lv_outlet_cd outlet8,JJOINN9.lv_outlet_cd outlet9,JJOINN10.lv_outlet_cd outlet10,JJOINN11.lv_outlet_cd outlet11,
       JJOINN12.lv_outlet_cd outlet12,JJOINN13.lv_outlet_cd outlet13,
       JJOINN14.lv_outlet_cd outlet14,JJOINN15.lv_outlet_cd outlet15,JJOINN16.lv_outlet_cd outlet16,JJOINN17.lv_outlet_cd outlet17,JJOINN18.lv_outlet_cd outlet18,JJOINN19.lv_outlet_cd outlet19,
       JJOINN20.lv_outlet_cd outlet20,JJOINN21.lv_outlet_cd outlet21,JJOINN22.lv_outlet_cd outlet22,
       
       
       
       ar.list_code,
    
     

       CURRENT_DATE AS TRANS_DATE,
       'INR' AS ARST_CURRENCY_CODE,
       

       

       'USER' AS ARST_CONVERSION_TYPE,

       '1' AS ARST_CONVERSION_RATE,
       
      gd.Liability_amount_debt,
      

       CASE am.channel
              WHEN 'COM'THEN 'PRAGATI CREDIT AGAINST POINTS REDEEMED IN '
             ELSE 'MARUTI SUZUKI REWARDS CREDIT AGAINST POINTS REDEEMED IN ' END
       || SUBSTRING(gd.trans_date, 4) AS ARST_REMARKS,



       'S' AS ARST_ACCOUNT_TYPE,

       'N' AS ARST_FUND_VALIDATION,


       'LINE' AS LINE_TYPE,

       '1' AS LINE_NUMBER,
       " " AS ARST_LINK_LINE,

  
        'R' AS ARST_PROCESS_FLAG,

       'MISC' AS ARST_PMOD_CODE,

       ar.list_code AS CARD_TYPE,

       am.channel,

       DATE_FORMAT(gd.trans_date, 'MMM') AS CREATION_MONTH,

       CASE DATE_FORMAT(gd.trans_date, 'MMM')
           WHEN 'JAN' THEN YEAR(gd.trans_date) - 1
           WHEN 'FEB' THEN YEAR(gd.trans_date) - 1
           WHEN 'MAR' THEN YEAR(gd.trans_date) - 1
           ELSE YEAR(gd.trans_date)
       END AS FIN_YEAR,

      gd.trans_date AS ARST_CREATION_DATE,

       " " AS TRANSFER_FLAG,

       am.parent_group,

       am.region_cd



       



FROM am_dealer_loc am

LEFT OUTER JOIN 
     df1 JOIN1
     ON am.mul_dealer_cd = JOIN1.mul_dealer_cd
     and am.for_cd = JOIN1.for_cd
     and am.outlet_cd = JOIN1.outlet_cd
     
LEFT OUTER JOIN 
     df2 JOIN2
     ON am.mul_dealer_cd = JOIN2.dealer_cd
     and am.for_cd = JOIN2.for_cd
     and am.outlet_cd = JOIN2.outlet_cd  
     
LEFT OUTER JOIN 
     df3 JOIN3
     ON am.mul_dealer_cd = JOIN3.dealer_cd
     and am.for_cd = JOIN3.for_cd
     and am.outlet_cd = JOIN3.outlet_cd 
     
LEFT OUTER JOIN 
     df4 JOIN4
     ON am.mul_dealer_cd = JOIN4.dealer_cd
     and am.for_cd = JOIN4.for_cd
     and am.outlet_cd = JOIN4.outlet_cd     
LEFT OUTER JOIN 
     df5 JOIN5
     ON am.mul_dealer_cd = JOIN5.dealer_cd
     and am.for_cd = JOIN5.for_cd
     and am.outlet_cd = JOIN5.outlet_cd 
     
LEFT OUTER JOIN 
     df6 JOIN6
     ON am.mul_dealer_cd = JOIN6.dealer_cd
     and am.for_cd = JOIN6.for_cd
     and am.outlet_cd = JOIN6.outlet_cd

LEFT OUTER JOIN 
     df7 JOIN7
     ON am.mul_dealer_cd = JOIN7.dealer_cd
     and am.for_cd = JOIN7.for_cd
LEFT OUTER JOIN 
     df8 JOIN8
     ON am.mul_dealer_cd = JOIN8.dealer_cd
     and am.for_cd = JOIN8.for_cd
LEFT OUTER JOIN 
     df9 JOIN9
     ON am.mul_dealer_cd = JOIN9.dealer_cd
     and am.for_cd = JOIN9.for_cd 
LEFT OUTER JOIN 
     df10 JOIN10
     ON am.mul_dealer_cd = JOIN10.dealer_cd
     and am.for_cd = JOIN10.for_cd  
     
LEFT OUTER JOIN 
     df11 JOIN11
     ON am.mul_dealer_cd = JOIN11.dealer_cd   
LEFT OUTER JOIN 
     df12 JOIN12
     ON am.mul_dealer_cd = JOIN12.dealer_cd  
LEFT OUTER JOIN 
     df13 JOIN13
     ON am.mul_dealer_cd = JOIN13.dealer_cd 
LEFT OUTER JOIN 
     df14 JOIN14
     ON am.mul_dealer_cd = JOIN14.dealer_cd
     
LEFT OUTER JOIN 
     df15 JOIN15
     ON am.for_cd = JOIN15.for_cd   
LEFT OUTER JOIN 
     df16 JOIN16
     ON am.for_cd = JOIN16.for_cd    
LEFT OUTER JOIN 
     df17 JOIN17
     ON am.for_cd = JOIN17.for_cd 
LEFT OUTER JOIN 
     df18 JOIN18
     ON am.for_cd = JOIN18.for_cd
     
LEFT OUTER JOIN 
     df19 JOIN19
     ON am.mul_dealer_cd = JOIN19.dealer_cd
     and am.for_cd = JOIN19.for_cd
     and am.outlet_cd = JOIN19.outlet_cd 
LEFT OUTER JOIN 
     df20 JOIN20
     ON am.mul_dealer_cd = JOIN20.dealer_cd
     and am.for_cd = JOIN20.for_cd
     and am.outlet_cd = JOIN20.outlet_cd   
LEFT OUTER JOIN 
     df21 JOIN21
     ON am.mul_dealer_cd = JOIN21.dealer_cd
     and am.for_cd = JOIN21.for_cd
     and am.outlet_cd = JOIN21.outlet_cd 
LEFT OUTER JOIN 
     df22 JOIN22
     ON am.mul_dealer_cd = JOIN22.dealer_cd
     and am.for_cd = JOIN22.for_cd
     and am.outlet_cd = JOIN22.outlet_cd 
     
     
LEFT OUTER JOIN 
     ddf1 JOINN1
     ON am.mul_dealer_cd = JOINN1.mul_dealer_cd
     and am.for_cd = JOINN1.for_cd
     and am.outlet_cd = JOINN1.outlet_cd
     
LEFT OUTER JOIN 
     ddf2 JOINN2
     ON am.mul_dealer_cd = JOINN2.dealer_cd
     and am.for_cd = JOINN2.lv_for_cd
     and am.outlet_cd = JOINN2.outlet_cd  
     
LEFT OUTER JOIN 
     ddf3 JOINN3
     ON am.mul_dealer_cd = JOINN3.dealer_cd
     and am.for_cd = JOINN3.for_cd
     and am.outlet_cd = JOINN3.outlet_cd 
     
LEFT OUTER JOIN 
     ddf4 JOINN4
     ON am.mul_dealer_cd = JOINN4.dealer_cd
     and am.for_cd = JOINN4.for_cd
     and am.outlet_cd = JOINN4.outlet_cd     
LEFT OUTER JOIN 
     ddf5 JOINN5
     ON am.mul_dealer_cd = JOINN5.dealer_cd
     and am.for_cd = JOINN5.for_cd
     and am.outlet_cd = JOINN5.outlet_cd 
     
LEFT OUTER JOIN 
     ddf6 JOINN6
     ON am.mul_dealer_cd = JOINN6.dealer_cd
     and am.for_cd = JOINN6.for_cd
     and am.outlet_cd = JOINN6.outlet_cd

LEFT OUTER JOIN 
     ddf7 JOINN7
     ON am.mul_dealer_cd = JOINN7.dealer_cd
     and am.for_cd = JOINN7.for_cd
LEFT OUTER JOIN 
     ddf8 JOINN8
     ON am.mul_dealer_cd = JOINN8.dealer_cd
     and am.for_cd = JOINN8.for_cd
LEFT OUTER JOIN 
     ddf9 JOINN9
     ON am.mul_dealer_cd = JOINN9.dealer_cd
     and am.for_cd = JOINN9.for_cd 
LEFT OUTER JOIN 
     ddf10 JOINN10
     ON am.mul_dealer_cd = JOINN10.dealer_cd
     and am.for_cd = JOINN10.for_cd  
     
LEFT OUTER JOIN 
     ddf11 JOINN11
     ON am.mul_dealer_cd = JOINN11.dealer_cd   
LEFT OUTER JOIN 
     ddf12 JOINN12
     ON am.mul_dealer_cd = JOINN12.dealer_cd  
LEFT OUTER JOIN 
     ddf13 JOINN13
     ON am.mul_dealer_cd = JOINN13.dealer_cd 
LEFT OUTER JOIN 
     ddf14 JOINN14
     ON am.mul_dealer_cd = JOINN14.dealer_cd
     
LEFT OUTER JOIN 
     ddf15 JOINN15
     ON am.for_cd = JOINN15.for_cd   
LEFT OUTER JOIN 
     ddf16 JOINN16
     ON am.for_cd = JOINN16.for_cd    
LEFT OUTER JOIN 
     ddf17 JOINN17
     ON am.for_cd = JOINN17.for_cd 
LEFT OUTER JOIN 
     ddf18 JOINN18
     ON am.for_cd = JOINN18.for_cd
     
LEFT OUTER JOIN 
     ddf19 JOINN19
     ON am.mul_dealer_cd = JOINN19.dealer_cd
     and am.for_cd = JOINN19.for_cd
     and am.outlet_cd = JOINN19.outlet_cd 
LEFT OUTER JOIN 
     ddf20 JOINN20
     ON am.mul_dealer_cd = JOINN20.dealer_cd
     and am.for_cd = JOINN20.for_cd
     and am.outlet_cd = JOINN20.outlet_cd   
LEFT OUTER JOIN 
     ddf21 JOINN21
     ON am.mul_dealer_cd = JOINN21.dealer_cd
     and am.for_cd = JOINN21.for_cd
     and am.outlet_cd = JOINN21.outlet_cd 
LEFT OUTER JOIN 
     ddf22 JOINN22
     ON am.mul_dealer_cd = JOINN22.dealer_cd
     and am.for_cd = JOINN22.for_cd
     and am.outlet_cd = JOINN22.outlet_cd      
     




LEFT OUTER JOIN 
     ddff1 JJOINN1
     ON am.mul_dealer_cd = JJOINN1.mul_dealer_cd
     and am.for_cd = JJOINN1.for_cd
     and am.outlet_cd = JJOINN1.outlet_cd
     
LEFT OUTER JOIN 
     ddff2 JJOINN2
     ON am.mul_dealer_cd = JJOINN2.dealer_cd
     and am.for_cd = JJOINN2.for_cd
     and am.outlet_cd = JJOINN2.outlet_cd  
     
LEFT OUTER JOIN 
     ddff3 JJOINN3
     ON am.mul_dealer_cd = JJOINN3.dealer_cd
     and am.for_cd = JJOINN3.for_cd
     and am.outlet_cd = JJOINN3.outlet_cd 
     
LEFT OUTER JOIN 
     ddff4 JJOINN4
     ON am.mul_dealer_cd = JJOINN4.dealer_cd
     and am.for_cd = JJOINN4.for_cd
     and am.outlet_cd = JJOINN4.outlet_cd     
LEFT OUTER JOIN 
     ddff5 JJOINN5
     ON am.mul_dealer_cd = JJOINN5.dealer_cd
     and am.for_cd = JJOINN5.for_cd
     and am.outlet_cd = JJOINN5.outlet_cd 
     
LEFT OUTER JOIN 
     ddff6 JJOINN6
     ON am.mul_dealer_cd = JJOINN6.dealer_cd
     and am.for_cd = JJOINN6.for_cd
     and am.outlet_cd = JJOINN6.outlet_cd

LEFT OUTER JOIN 
     ddff7 JJOINN7
     ON am.mul_dealer_cd = JJOINN7.dealer_cd
     and am.for_cd = JJOINN7.for_cd
LEFT OUTER JOIN 
     ddff8 JJOINN8
     ON am.mul_dealer_cd = JJOINN8.dealer_cd
     and am.for_cd = JJOINN8.for_cd
LEFT OUTER JOIN 
     ddff9 JJOINN9
     ON am.mul_dealer_cd = JJOINN9.dealer_cd
     and am.for_cd = JJOINN9.for_cd 
LEFT OUTER JOIN 
     ddff10 JJOINN10
     ON am.mul_dealer_cd = JJOINN10.dealer_cd
     and am.for_cd = JJOINN10.for_cd  
     
LEFT OUTER JOIN 
     ddff11 JJOINN11
     ON am.mul_dealer_cd = JJOINN11.dealer_cd   
LEFT OUTER JOIN 
     ddff12 JJOINN12
     ON am.mul_dealer_cd = JJOINN12.dealer_cd  
LEFT OUTER JOIN 
     ddff13 JJOINN13
     ON am.mul_dealer_cd = JJOINN13.dealer_cd 
LEFT OUTER JOIN 
     ddff14 JJOINN14
     ON am.mul_dealer_cd = JJOINN14.dealer_cd
     
LEFT OUTER JOIN 
     ddff15 JJOINN15
     ON am.for_cd = JJOINN15.for_cd   
LEFT OUTER JOIN 
     ddff16 JJOINN16
     ON am.for_cd = JJOINN16.for_cd    
LEFT OUTER JOIN 
     ddff17 JJOINN17
     ON am.for_cd = JJOINN17.for_cd 
LEFT OUTER JOIN 
     ddff18 JJOINN18
     ON am.for_cd = JJOINN18.for_cd
     
LEFT OUTER JOIN 
     ddff19 JJOINN19
     ON am.mul_dealer_cd = JJOINN19.dealer_cd
     and am.for_cd = JJOINN19.for_cd
     and am.outlet_cd = JJOINN19.outlet_cd 
LEFT OUTER JOIN 
     ddff20 JJOINN20
     ON am.mul_dealer_cd = JJOINN20.dealer_cd
     and am.for_cd = JJOINN20.for_cd
     and am.outlet_cd = JJOINN20.outlet_cd   
LEFT OUTER JOIN 
     ddff21 JJOINN21
     ON am.mul_dealer_cd = JJOINN21.dealer_cd
     and am.for_cd = JJOINN21.for_cd
     and am.outlet_cd = JJOINN21.outlet_cd 
LEFT OUTER JOIN 
     ddff22 JJOINN22
     ON am.mul_dealer_cd = JJOINN22.dealer_cd
     and am.for_cd = JJOINN22.for_cd
     and am.outlet_cd = JJOINN22.outlet_cd 

JOIN    
     rec
     on am.parent_group = rec.dr_parent_group
        AND am.dealer_map_cd = rec.dr_dlr_map_cd
        AND am.loc_cd = rec.dr_loc_cd

JOIN 
    gd_loyalty_trans gd  ON gd.dr_parent_group = am.parent_group 
                          AND gd.dr_dlr_map_cd = am.dealer_map_cd 
                         AND gd.dr_loc_cd = am.loc_cd
JOIN 
    gd_loyalty_enrol le ON le.card_num = gd.card_num
    
JOIN 
    AM_LIST_RANGE AR ON AR.principal_map_cd = am.principal_map_cd 
                    AND AR.list_name = 'CARD_TYPE' 
                    AND AR.list_code = le.card_type 
                    AND IFNULL(ar.list_flag, 'Y') = 'Y' 
                    AND AR.list_grp_code = le.channel
    
                         

WHERE gd.debit_pts IS NOT NULL
AND gd.trans_date >= '2020-06-21'
AND gd.trans_date < '2020-07-21'
---AND am.mul_dealer_cd != '9967'



          
 """)


#df_query6.printSchema()
#df_query6.count()
#df_query6.show()
#query7 with trans_date

###2nd query of CREDIT

df_query6.createOrReplaceTempView("df_query6")

df_query7 = spark.sql("""
SELECT 
 
 source,

        MUL_DEALER_CD,
        FOR_CD,
        OUTLET_CD,
        old_trans_date,
        lv_parent_group,
        lv_parent_groupFOR,
        lv_parent_groupOUTLET,
        
        dlr15, dlr16, dlr17, dlr18, dlr19, dlr20,
     dlr21, dlr22,
       for15,for16,for17,for18,for19,for20,for21,for22,
       outlet15,outlet16,outlet17,outlet18,outlet19,outlet20,outlet21,outlet22,
       
     coalesce(dlr2, dlr3, dlr4,  dlr5,  dlr6, dlr7, dlr8,
     dlr9, dlr10,dlr11, dlr12 , dlr13, dlr14) as ARST_BILL_TO_DEALER_CODE1,
     
     coalesce(for2, for3, for4,  for5,  for6, for7, for8,
     for9, for10,for11, for12 , for13, for14) as ARST_BILL_TO_FOR_CODE1,
     
     coalesce(outlet2, outlet3, outlet4,  outlet5,  outlet6, outlet7, outlet8,
     outlet9, outlet10,outlet11, outlet12 , outlet13, outlet14) as ARST_BILL_TO_OUTLET_CODE1,
     
       list_code,
       TRANS_DATE,
       ARST_CURRENCY_CODE,
       TRANS_DATE,

       ARST_CURRENCY_CODE,

        ARST_CONVERSION_TYPE,

       ARST_CONVERSION_RATE,
       
      ROUND(SUM(Liability_amount_debt), 2) AS AMOUNT,
      

        ARST_REMARKS,
ARST_ACCOUNT_TYPE,
ARST_FUND_VALIDATION,
LINE_TYPE,
LINE_NUMBER,
ARST_LINK_LINE,
ARST_PROCESS_FLAG,
ARST_PMOD_CODE,
CARD_TYPE,
channel,
CREATION_MONTH,
FIN_YEAR,
ARST_CREATION_DATE,
TRANSFER_FLAG,
parent_group,
region_cd
       
       from df_query6
       
       GROUP BY
       SOURCE,
       MUL_DEALER_CD,
        FOR_CD, 
        OUTLET_CD,
        channel,
       CARD_TYPE,
          CREATION_MONTH,
          TRANS_DATE, 
          parent_group, 
          region_cd,
         dlr2, dlr3,dlr4,dlr5,dlr6,dlr7,dlr8,dlr9,
         dlr10,dlr11,dlr12,dlr13,dlr14,dlr15,dlr16,
         dlr17,
         dlr18,dlr19,dlr20,dlr21,dlr22,
         
         for2, for3, for4,  for5,  for6, for7, for8,
     for9, for10,for11, for12 , for13, for14,for15,for16,for17,for18,for19,for20,for21,for22,
     outlet2, outlet3, outlet4,  outlet5,  outlet6, outlet7, outlet8,
     outlet9, outlet10,outlet11, outlet12 , outlet13, outlet14,
     outlet15,outlet16,outlet17,outlet18,outlet19,outlet20,outlet21,outlet22,
         FIN_YEAR,
          ARST_REMARKS,
ARST_ACCOUNT_TYPE,
ARST_FUND_VALIDATION,
LINE_TYPE,
LINE_NUMBER,
ARST_LINK_LINE,
ARST_PROCESS_FLAG,
ARST_PMOD_CODE,
CARD_TYPE,
channel,
CREATION_MONTH,
FIN_YEAR,
ARST_CREATION_DATE,
TRANSFER_FLAG,
parent_group,
region_cd,
ARST_CURRENCY_CODE,
ARST_CONVERSION_TYPE,
ARST_CONVERSION_RATE,
lv_parent_group,
lv_parent_groupFOR,
lv_parent_groupOUTLET,
old_trans_date,
list_code
 """)


#df_query7.printSchema()
#df_query7.count()
#df_query7.show()
##query8 with transdate---

df_query7.createOrReplaceTempView("df_query7")
df_query8 = spark.sql("""
SELECT 
 
 source,

        MUL_DEALER_CD,
        FOR_CD,
        OUTLET_CD,
        old_trans_date,
        
       
     coalesce(ARST_BILL_TO_DEALER_CODE1,lv_parent_group,dlr15, dlr16, dlr17, dlr18, dlr19, dlr20,
     dlr21, dlr22) as ARST_BILL_TO_DEALER_CODE,
     
      coalesce(ARST_BILL_TO_FOR_CODE1,lv_parent_groupFOR, for15,for16,for17,for18,for19,for20,for21,for22) as ARST_BILL_TO_FOR_CODE,
      
      coalesce(ARST_BILL_TO_OUTLET_CODE1,lv_parent_groupOUTLET,outlet15,outlet16,outlet17,outlet18,outlet19,outlet20,outlet21,outlet22) as ARST_BILL_TO_OUTLET_CODE,
       list_code,
       TRANS_DATE,
       ARST_CURRENCY_CODE,
     

    

        ARST_CONVERSION_TYPE,
        ARST_CONVERSION_RATE,

      
       
       AMOUNT,
      

        ARST_REMARKS,
ARST_ACCOUNT_TYPE,
ARST_FUND_VALIDATION,
LINE_TYPE,
LINE_NUMBER,
ARST_LINK_LINE,
ARST_PROCESS_FLAG,
ARST_PMOD_CODE,
CARD_TYPE,
channel,
CREATION_MONTH,
FIN_YEAR,
ARST_CREATION_DATE,
TRANSFER_FLAG,
parent_group,
region_cd
       
       from df_query7
       
       
 """)


df_query8.printSchema()
#df_query8.count()
#df_query8.show()
##Adding columns to query8---
df_query8 = df_query8.withColumn('ARST_CUSTOMER_CATEGORY', F.substring(col('ARST_BILL_TO_DEALER_CODE'), 1,3))


##Adding LV_batch number
df_query8 = df_query8.withColumn('financial_year', when(month('old_trans_date') < 4, year('old_trans_date')-1).otherwise(year('old_trans_date')))
df_query8= df_query8.withColumn('financial_month', when(month('old_trans_date') < 4, concat(lit('0'),month('old_trans_date') + 9)).otherwise(concat(lit('00'),month('TRANS_DATE') - 3)))
df_query8 = df_query8.withColumn('lv_batch_number', concat(lit('CBT'),col('financial_year'),col('financial_month')))

df_query8.printSchema()
#####Adding Column---TRANS_NUM----to query8

df_query8 = df_query8.withColumn('channel_mod', when(col('channel') == 'COM','C').when(col('channel') == 'EXC','N').when(col('channel') == 'NRM','A'))\
                    .withColumn('TRANS_NUM', concat(F.substring(col('lv_batch_number'),1,5),col('list_code'),col('channel_mod'),lit('-'),F.substring(col('ARST_BILL_TO_DEALER_CODE'),1,5),lit('-'),col('ARST_BILL_TO_OUTLET_CODE')))
##Adding Column ELEMENT

df_query8=  df_query8.withColumn('Element',when(col('ARST_CUSTOMER_CATEGORY')=='VCF','AC_RRPD').otherwise('AC_RRPSD'))

#UNION o Query5 and Query8

df_query5.createOrReplaceTempView("df_query5")
df_query8.createOrReplaceTempView("df_query8")

df = df_query5.unionAll(df_query8)
###SET AND UPDATE

df = df.withColumn('ARST_BILL_TO_FOR_CODE', when(col('ARST_CUSTOMER_CATEGORY')== 'MAS', '00')).withColumn('ARST_BILL_TO_OUTLET_CODE', when(col('ARST_CUSTOMER_CATEGORY')== 'MAS', '00')).withColumn('ARST_CUSTOMER_CATEGORY', when(col('ARST_CUSTOMER_CATEGORY')== 'MAS','MASS')).withColumn('ARST_BILL_TO_OUTLET_CODE', when(col('ARST_CUSTOMER_CATEGORY').isin(['DDT','DMM']),'000'))

df.createOrReplaceTempView("df")

Df_Summary_data = spark.sql(""" SELECT 
             SOURCE,
             ARST_BILL_TO_DEALER_CODE,
             ARST_BILL_TO_FOR_CODE,
             ARST_BILL_TO_OUTLET_CODE,
             ARST_CUSTOMER_CATEGORY,
             TRANS_NUM,
             TRANS_DATE,
             ARST_CURRENCY_CODE,
             ARST_CONVERSION_TYPE,
             ARST_CONVERSION_RATE,
             SUM(AMOUNT),
             ARST_REMARKS,
             ELEMENT,
             ARST_ACCOUNT_TYPE,
             ARST_FUND_VALIDATION,
             Lv_batch_number as ARST_BATCH_NUMBER,
             LINE_TYPE,
             LINE_NUMBER,
             ARST_LINK_LINE,
             ARST_PROCESS_FLAG,
             ARST_PMOD_CODE,
             CARD_TYPE,
             CHANNEL,
             CREATION_MONTH,
             FIN_YEAR,
             ARST_CREATION_DATE,
             TRANSFER_FLAG,
             parent_group,
             region_cd
FROM df
WHERE  NVL(AMOUNT, 0) <> 0
GROUP BY 
                SOURCE,
                ARST_BILL_TO_DEALER_CODE,
                ARST_BILL_TO_FOR_CODE,
                ARST_BILL_TO_OUTLET_CODE,
                ARST_CUSTOMER_CATEGORY,
                TRANS_NUM,
                TRANS_DATE,
                ARST_CURRENCY_CODE,
                ARST_CONVERSION_TYPE,
                ARST_CONVERSION_RATE,
                ARST_REMARKS,
                ELEMENT,
                ARST_ACCOUNT_TYPE,
                ARST_FUND_VALIDATION,
                ARST_BATCH_NUMBER,
                LINE_TYPE,
                LINE_NUMBER,
                ARST_LINK_LINE,
                ARST_PROCESS_FLAG,
                ARST_PMOD_CODE,
                CARD_TYPE,
                CHANNEL,
                CREATION_MONTH,
                FIN_YEAR,
                ARST_CREATION_DATE,
                TRANSFER_FLAG,
                parent_group,
                region_cd
""")

#summary_df.show()

job.commit()