# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # Project - PID Streaming - Tomáš Mlynář - mlynatom@fel.cvut.cz
# MAGIC ---
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 20px;">
# MAGIC   <img src="https://raw.githubusercontent.com/animrichter/BDT_2023/master/data/assets/streaming.png" style="width: 1200">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC klidně jen zkusit přes vánoce a po vánocích kdyžtak je konzultae (ale chatovací nástroje by to měly zvládnout)
# MAGIC
# MAGIC na konec reportu uveďte zdroje - pro info kdo co používá (e.g. copilot, chatGPT+verze)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Kafka Connection

# COMMAND ----------

kafka_cluster = "b-3-public.felkafkamsk.56s6v1.c2.kafka.eu-central-1.amazonaws.com:9196,b-2-public.felkafkamsk.56s6v1.c2.kafka.eu-central-1.amazonaws.com:9196,b-1-public.felkafkamsk.56s6v1.c2.kafka.eu-central-1.amazonaws.com:9196"

topic = "fel-pid-topic"

# COMMAND ----------

#read data as a stream
raw = (
    spark
        .readStream
        .format('kafka')
        .option('kafka.bootstrap.servers', kafka_cluster)
        .option('subscribe', topic)
        .option('startingOffsets', "earliest")
        .option('kafka.sasl.mechanism', 'SCRAM-SHA-512')
        .option('kafka.security.protocol', 'SASL_SSL')
        .option('kafka.sasl.jaas.config', 'kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="FELPIDUSER" password="dzs3c1vldy6np5";')
        .load()
)

#nebo lze jen sparkread a ten si sáhne do kafky a stáhne co má aktuálně k dispozici - lze nastvit offset (teď kafka obsahuje všechno)

checkpoint = '/mnt/pid/checkpoint_file.txt'

(raw.writeStream
   .format("delta")
   .outputMode("append")
   .option("checkpointLocation", checkpoint)
   .toTable("fel_pid_topic_data")
)

# COMMAND ----------

#read data as a batch from kafka
raw = (
    spark
        .read #only change
        .format('kafka')
        .option('kafka.bootstrap.servers', kafka_cluster)
        .option('subscribe', topic)
        .option('startingOffsets', "earliest") #starting offset - the kafka is set to save everything
        .option('kafka.sasl.mechanism', 'SCRAM-SHA-512')
        .option('kafka.security.protocol', 'SASL_SSL')
        .option('kafka.sasl.jaas.config', 'kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="FELPIDUSER" password="dzs3c1vldy6np5";')
        .load()
)

checkpoint = '/mnt/pid/checkpoint_file_batch.txt'

#different write
(raw.write
   .format("delta")
   .option("checkpointLocation", checkpoint)
   .saveAsTable("fel_pid_topic_data_batch")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select string(value), * from fel_pid_topic_data_batch order by `timestamp` desc

# COMMAND ----------

#data schema for parsing raw data
schema = 'array<struct<geometry: struct<coordinates: array<double>, type: string>, properties: struct<last_position: struct<bearing: int, delay: struct<actual: int, last_stop_arrival: int, last_stop_departure: int>, is_canceled: string, last_stop: struct<arrival_time: string, departure_time: string, id: string, sequence: int>, next_stop: struct<arrival_time: string, departure_time: string, id: string, sequence: int>, origin_timestamp: string, shape_dist_traveled: string, speed: string, state_position: string, tracking: boolean>, trip: struct<agency_name: struct<real: string, scheduled: string>, cis: struct<line_id: string, trip_number: string>, gtfs: struct<route_id: string, route_short_name: string, route_type: int, trip_headsign: string, trip_id: string, trip_short_name: string>, origin_route_name: string, sequence_id: int, start_timestamp: string, vehicle_registration_number: string, vehicle_type: struct<description_cs: string, description_en: string, id: int>, wheelchair_accessible: boolean, air_conditioned: boolean, usb_chargers: boolean>>, type: string>>'
