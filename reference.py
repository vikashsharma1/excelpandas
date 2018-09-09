# Dataframe from SQl
df_inventory = spark.sql("Select * from blackwater.inventory")

# Unique values in columns
display(df_inventory.select('mInvenGlAmt').distinct())

# Count number of distinct values in a coloumn
df_inventory.agg(countDistinct(col("iStatus")).alias("count")).show()

# Count number of distinct values in each coloumn 
from pyspark.sql.functions import col, countDistinct
display(df_inventory.agg(*(countDistinct(col(c)).alias(c) for c in df_inventory.columns)))

# Finding the count of null Values in all coloumns
from pyspark.sql.functions import isnan, when, count, col
df_inventory.select([count(when(col(c).isNull(), c)).alias(c) for c in df_inventory.columns]).show()

# Select coloumn Values
display(df_inventory.select('iInventoryType').distinct().collect())
# Conditional Select
columns = [ column for column in df.columns if len(df.select(column).distinct().collect()) > 1 ]


# taking the count of Null/NaN in each column of dataframe
data.isnull().sum()

# if want to know the total
data.isnull().sum().sum()

# if want to check in any particular column
data['Dependents'].isnull().sum()

# isnull or notnull
data.isnull().head(6)


# use of any
data.isnull().values.any()


df.groupBy('state').count().show()
df.groupby(['x1','x2'], as_index=False).count()
 df.groupby(['ColA','ColB']).size().reset_index(name='Count')

df2 = df.distinct()
or

df2 = df.drop_duplicates()

# use of all
data.isnull().values.all()
# Sorting 
sort(desc("ExternalDSVehicleClassId")))

i =  df_inventory.columns
v = df_vehicle.columns
len(i), len(v)
c =  set(i).intersection(v)
 i = list(set(i) - c)
v = list(set(v) - c)
len(i), len(v)\


# Worksheet size: Maximum of 1,048,576 rows and 16,384 columns

# 1. inventory : 18,989,861 
#     	1.89 Crore

# 2. vehicle : 179736401
# 	17.97 Cr

# 3. servicedetail : 1302252138
# 	1.3 Arab

# 4.site_address :  6714

# Databricks notebook source
print("Hello")

# COMMAND ----------

df_siteaddress = spark.sql("SELECT * from blackwater.site_address")

# COMMAND ----------

df_siteaddress.count()

# COMMAND ----------

# Dataframe from SQl
df_vehicle= spark.sql("Select * from blackwater.vehicle")

# COMMAND ----------

from pyspark.sql.functions import col, countDistinct
display(df_vehicle.agg(*(countDistinct(col(c)).alias(c) for c in df_vehicle.columns)))

# COMMAND ----------

# Finding the count of null Values in all coloumns
from pyspark.sql.functions import isnan, when, count, col
display(df_vehicle.select([count(when(col(c).isNull(), c)).alias(c) for c in df_vehicle.columns]))

# COMMAND ----------

df_vehicle.count()

# COMMAND ----------

len(df_vehicle.columns)


# COMMAND ----------

display(df_vehicle.select().distinct())

# COMMAND ----------

idxCategories


# COMMAND ----------

categories = {}
idxCategories = [3, 16, 17, 18]
for i in idxCategories: ##idxCategories contains indexes of rows that contains categorical data
    distinctValues = df_vehicle.map(lambda x : x[i]).distinct().collect()
    categories[i] = distinctValues
    
print(categories[0])
print(categories[1])
print(categories[2])

# COMMAND ----------

from functools import reduce
product = reduce((lambda x, y: x * y), [1, 2, 3, 4])
print(product)


# COMMAND ----------

display([1, 2, 3])

# COMMAND ----------

df_vehicle.columns


# COMMAND ----------

display(df_vehicle.select('iStatusId').distinct())

# COMMAND ----------

df_vehicle.select('iStatusId').distinct().count()

# COMMAND ----------

a = [df_vehicle.select((col(c))).distinct().alias(c) for c in df_vehicle.columns]

# COMMAND ----------

a[0
]

# COMMAND ----------

display(df_vehicle[a[0]])

# COMMAND ----------

a[0].show()

# COMMAND ----------

a[3].show()

# COMMAND ----------

 len(a[3])

# COMMAND ----------

a[3].count()

# COMMAND ----------

display(a[3])

# COMMAND ----------

display(a[0], a[16])

# COMMAND ----------

df_vehicle = spark.sql("select top 10 from blackwater.inventory")

# COMMAND ----------

df_inventory = spark.sql("Select * from blackwater.inventory")

# COMMAND ----------

i = df_inventory.columns
v = df_vehicle.columns

# COMMAND ----------

len(i), len(v)

# COMMAND ----------

len(i + v)

# COMMAND ----------

len(set(i).intersection(set(v)))

# COMMAND ----------

c = set(i).intersection(set(v))

# COMMAND ----------

c


# COMMAND ----------

i = set(i) - c

# COMMAND ----------

i

# COMMAND ----------

v = set(v) - c

# COMMAND ----------

display(v)


# COMMAND ----------

v = list(v)
len(v)

# COMMAND ----------

df_v = sqlContext.createDataFrame([v])
display(df_v)

# COMMAND ----------

type(v
)

# COMMAND ----------

v


# COMMAND ----------

c


# COMMAND ----------

i =  df_inventory.columns
v = df_vehicle.columns
len(i), len(v)


# COMMAND ----------

c =  set(i).intersection(v)
i = list(set(i) - c)
v = list(set(v) - c)
len(i), len(v)

# COMMAND ----------

display(sqlContext.createDataFrame([i]))

# COMMAND ----------

display(sqlContext.createDataFrame([v]))

# COMMAND ----------

display(sqlContext.createDataFrame([list(c)]))

# COMMAND ----------

display(spark.createDataFrame([i]))

# COMMAND ----------

display(df_vehicle['vchStockNo'])

# COMMAND ----------

# MAGIC %sql
# MAGIC select  vchStockNo from blackwater.inventory limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from blackwater.vehicle where vchStockNO='F3816' ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct vchStockNO, count(*) from blackwater.inventory group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from blackwater.inventory where vchStockNO='F3816' ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC  from blackwater.inventory where vchStockNO='F3816' ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC  from blackwater.vehicle where vchStockNO='F3816' ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct iPurchaseType, count(*) from blackwater.vehicle group by 1; 

# COMMAND ----------

df_vehicle = spark.sql("Select * from blackwater.vehicle")

# COMMAND ----------

df_vehicle.count()

# COMMAND ----------

df2 = df_vehicle.distinct()

# COMMAND ----------

df2.count()


# COMMAND ----------

179736401 - 179683244

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct dtModifiedDate, count(*) from blackwater.vehicle group by 1 order by 1 desc; 

# COMMAND ----------

df_vehicle = spark.sql("Select * from blackwater.vehicle")

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct vchFuelType, count(*) from blackwater.vehicle group by 1 order by 2 desc; 

# COMMAND ----------

# MAGIC %sql
# MAGIC select  mLifePrem,mLifePremium from blackwater.vehicle where mLifePrem != mLifePremium ; 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct vchFuelType, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct vchVehicleClass, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct mHoldback, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct iSrvcMaintMiles, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct iSrvcMaintTerm, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct vchSrvcMaintPlan, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct vchBookMake, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct vchBookModel, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct vchBookTrim, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct vchBookCategory, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct vchBookYear, count(*)
# MAGIC from  blackwater.vehicle where vchBookYear > 2017 group by 1 order by 2 desc  ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct vchSalesPerson1ID, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct vchSalesPerson2ID, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct vchMakeCode, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct iDaysToSold, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct mExtWarRes, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct vchExtWarrantyContractNo, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct dtDeliveredDate, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 1 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct dtExtWarrantyExpire, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 1 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct vchVSCNo, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct dtVSCExpire, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 1 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct vchUnitNo, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct mValuation, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct dtValuationDate, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 1 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct mEquity, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct dtEquityDate, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 1 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct iStyleId, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc; 

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct AmortTerm, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct FriendlyTrim, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct VehicleDescriptionUrl, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct NumberOfPayments, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct PaymentFrequencyCode, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct mFinalPay, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct RRCurrentMilesUpdateDate, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct MPGHwy, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct MPGCity, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct NextServiceDate, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct OverMileageFlatPenalty, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct OverMileagePerMilePenalty, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct vchBookId, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct AllowedMilesPerYear, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct AllowedMilesPerYear, count(*)
# MAGIC from  blackwater.vehicle group by 1 order by 2 desc;

# COMMAND ----------

df_sa =  spark.sql("SELECT * from blackwater.site_address")

# COMMAND ----------

pd_df_sa = df_sa.toPandas()

# COMMAND ----------

# MAGIC %pdb.distinct()

# COMMAND ----------

pd_df_sa.groupby('chState').count()

# COMMAND ----------

pd_df_sa.columns

# COMMAND ----------

df_sa.size()

# COMMAND ----------

pd_df_sa.memory_usage(index=True).sum()

# COMMAND ----------

241776/1024


# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct dtRODate, count(*) from blackwater.servicedetail group by 1 order by 1 asc

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct vchPostcode, count(*) from blackwater.servicedetail group by 1 order by 2 desc

# COMMAND ----------

df_inventory = spark.sql("Select * from blackwater.inventory")

# COMMAND ----------

df_sa = spark.sql("Select * from blackwater.site_address")

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct iSiteId , count(*) FROM blackwater.site_address group by 1 order by 2 desc ;

# COMMAND ----------

df_isiteid = df_sa.select('iSiteId').distinct()


# COMMAND ----------

df_in_isiteid =  df_inventory.select('iSiteId').distinct()

# COMMAND ----------

df_isiteid.count()

# COMMAND ----------

df_in_isiteid.count()

# COMMAND ----------

sa_in = set(list(df_isiteid)) - set(list(df_in_isiteid))

# COMMAND ----------

print(list(sa_in))

# COMMAND ----------

in_sa = set(list(df_in_isiteid)) - set(list(df_isiteid))

# COMMAND ----------

len(in_sa)

# COMMAND ----------

display(df_in_isiteid)

# COMMAND ----------

df_isiteid.toPandas() - df_in_isiteid.toPandas()

# COMMAND ----------

# pd_sa_sid = df_isiteid.toPandas()
pd_in_sid = df_in_isiteid.toPandas().astype(float)

# COMMAND ----------

print(list(pd_sa_sid.iSiteId) - list(pd_in_sid.iSiteId))

# COMMAND ----------

# MAGIC %sql 
# MAGIC select distinct iSiteId from blackwater.inventory where iSiteId like "NA";

# COMMAND ----------

from pyspark.sql.functions import isnan, when, count, col
display(df_inventory.select([count(when(col(c).isNull(), c)).alias(c) for c in df_inventory.columns]))

# COMMAND ----------

# MAGIC %sql
# MAGIC select  distinct concat(iInventory, '-', iSiteId), count(*) from blackwater.inventory group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from blackwater.inventory where iInventory is null and iSiteId is null ; 

# COMMAND ----------

df_in = spark.sql("Select iInventory, iSiteId  from blackwater.inventory ")
print(df_in.distinct().count())

# COMMAND ----------

df_in = df_in.na.fill(0)

# COMMAND ----------

df_in =  df_in.toPandas()

# COMMAND ----------

df_in.shape

# COMMAND ----------

df_in_2 = df_in.distinct()

# COMMAND ----------

18989861 - 18971348

# COMMAND ----------

# MAGIC %sql
# MAGIC select mBaseRetail,mMSRP,mBaseMSRP from  blackwater.inventory where mBaseRetail !=0 and mMSRP !=0 and mBaseMSRP !=0 
# MAGIC -- group by 1 order by 2 desc;

# COMMAND ----------

df = spark.sql("Select * from blackwater.inventory")

# COMMAND ----------

df.explain()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.count()

# COMMAND ----------

df.distinct().count()

# COMMAND ----------

18989861 - 18978990

# COMMAND ----------

df.schema

# COMMAND ----------

df.show()

# COMMAND ----------

display(df.groupby('IsConsignment').count().orderBy('count', ascending=0))

# COMMAND ----------

df = spark.sql("SELECT * from blackwater.inventory")


# COMMAND ----------

df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# COMMAND ----------

df.columns[0]

# COMMAND ----------

from pyspark.sql.functions import isnan, when, count, col
col(df.columns[0])

# COMMAND ----------

df.select(df.columns[0] < 0)

# COMMAND ----------

df.iStyleId < 0

# COMMAND ----------

df[df.iStyleId == "0"].count()

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct dtInsertDate, count(*) from blackwater.inventory group by 1 order by 1 asc ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct iSiteid, count(*) from blackwater.site_address group by 1 order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct iSiteid from blackwater.inventory where iSiteId not in (select distinct iSiteid from blackwater.site_address );

# COMMAND ----------


