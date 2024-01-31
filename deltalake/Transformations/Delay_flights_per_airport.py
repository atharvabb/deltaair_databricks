# Databricks notebook source
df = spark.sql("""select f.FlightNum,p.aircraft_type,p.manufacturer,a.city as origin_city,a.country origin_country,
a1.city dest_city,a1.country dest_country,case when f.ArrDelay>0 or f.DepDelay>0 then 1 else 0 end as is_delayed,f.dw_load_date 
from deltadb.flight f left join deltadb.planes p on f.TailNum=p.tailnum 
left join deltadb.airport a on a.airport_code=f.Origin left join 
deltadb.airport a1 on a1.airport_code=f.Dest """)



# COMMAND ----------

df.write.partitionBy("dw_load_date").mode("overwrite").saveAsTable("deltadb.app_view")
