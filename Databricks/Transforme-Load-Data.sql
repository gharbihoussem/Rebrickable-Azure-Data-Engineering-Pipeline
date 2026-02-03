#Transforme Data  :

#python :
display(dbutils.fs.ls("abfss://raw@ADF-name.dfs.demo.window.net"))

df=spark.read.format("json").load("link to the Rebrickable Minifigs File")

from pyspark.sql.functions import explode

df2=( df.withColumn("explodedArray" , explode(df.result) ) )
display(df2)

df2.createOrReplaceTempView("MyData")

---

#SQL:

with Duplicates as ( 

Select
  explodedArray.last_modifed_dt as LastModifiedDatetimeOriginal , 
  to_date ( explodedArray.last_modifed_dt ) as LastModifiedDate , 
  to_timestamp ( explodedArray.last_modifed_dt ) as LastModifiedDatetime , 
  explodedArray.name as Name,
  case 
  	when upper ( explodedArray.name ) like"%"TOY%" then "Toy"
  	when upper ( explodedArray.name ) like"%"DROID%" then "Droid"
  	else "Other" 
  end as MiniFigsType , 
  cast ( explodedArray.name_parts as int ) as NumberOfParts, 
  explodedArray.set_image_url as ImageURL , 
  explodedArray.set_num as SetNumber, 
  explodedArray.set_url as SetURL , 
  row_number ( ) over  ( partition by explodedArray.set_num order by to_timestamp ( explodedArray.last_modifed_dt ) Desc  ) as Rank
from 
	MyData 
)
select 
	*
from 
	Duplicates
where 
	Rank = 1 and ImageURL is not null 

---

#Load Data : 
#py
  _sqldf \
		.write \
		.format ("delta") \
		.save (" abfss://conformed@.............net / Rebrickable / Minifigs")

display ( spark.read.format("delta").load("abfss://conformed@.........Minifigs" ) )


#sql
Create database GharbiDB ;
 
Create table GharbiDB.Minifigs_External

 Location "abfss://conformed@adfs_name/Rebrickable/Minifigs"

as

Select
  explodedArray.last_modifed_dt as LastModifiedDatetimeOriginal , 
  to_date ( explodedArray.last_modifed_dt ) as LastModifiedDate , 
  to_timestamp ( explodedArray.last_modifed_dt ) as LastModifiedDatetime , 
  explodedArray.name as Name,
  case 
  	when upper ( explodedArray.name ) like"%"TOY%" then "Toy"
  	when upper ( explodedArray.name ) like"%"DROID%" then "Droid"
  	else "Other" 
  end as MiniFigsType , 
  cast ( explodedArray.name_parts as int ) as NumberOfParts, 
  explodedArray.set_image_url as ImageURL , 
  explodedArray.set_num as SetNumber, 
  explodedArray.set_url as SetURL
from 

	MyData 
 

