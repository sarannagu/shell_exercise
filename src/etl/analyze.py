
energy_data = spark.read.option("header", "true").option("inferSchema","true").csv('/Users/srivatsanprakash/Downloads/Data Engineering Test/Data/ConsumptionData/NMIG2.csv')
# read the folder
energy_data = spark.read.csv('/Users/srivatsanprakash/Downloads/Data Engineering Test/Data/ConsumptionData', header=True, inferSchema=True).withColumn('source_file_name', F.input_file_name())

meta_data = spark.read.csv('/Users/srivatsanprakash/Downloads/Data Engineering Test/Data/nmi_info.csv',header=True, inferSchema=True).withColumn('meta_data_file_name', F.input_file_name())

'''
|2017-11-06 11:30:00|file:///Users/srivatsanprakash/Downloads/Data%20Engineering%20Test/Data/ConsumptionData/NMIR2.csv|2017-11-06 11:30:00|NMIR2 |36.344            |kWh     |11  |NMIR2|WA   |30.0    |file:///Users/srivatsanprakash/Downloads/Data%20Engineering%20Test/Data/nmi_info.csv|2017-11-06 11:30:00|
|2018-01-30 16:00:00|file:///Users/srivatsanprakash/Downloads/Data%20Engineering%20Test/Data/ConsumptionData/NMIR2.csv|2018-01-30 16:00:00|NMIR2 |41.092            |kWh     |16  |NMIR2|WA   |30.0    |file:///Users/srivatsanprakash/Downloads/Data%20Engineering%20Test/Data/nmi_info.csv|2018-01-30 16:00:00|
windowSpec  = Window.partitionBy("nmi_id").orderBy("aest_time")
next_date = F.coalesce(F.lead("aest_time", 1).over(windowSpec), F.col("aest_time") + F.expr("interval 30 minutes"))
end_date_range = next_date - F.expr("interval 30 minutes")

energy_data.withColumn("Ranges", F.sequence(F.col("aest_time"), end_date_range, F.col("Interval)))\
  .withColumn("DATE", F.explode("Ranges"))\
  .withColumn("DATE", F.date_format("date", "dd/MM/yyyy"))\
  .drop("Ranges").show(truncate=False)

'''
corrupt_data = spark.read.option("header", "true").option("inferSchema","true").csv('/Users/srivatsanprakash/Downloads/Data Engineering Test/Data/ConsumptionData/NMIR2.csv')

corrupt_data.select(F.to_timestamp(F.lit('01/10/2017 00:00:00'),'dd/MM/yyyy HH:mm:ss')).show(10,False)
corrupt_data = corrupt_data.withColumn('new_time', F.to_timestamp('AESTTime','dd/MM/yyyy HH:mm:ss'))
corrupt_data = corrupt_data.withColumn('new_time', F.from_unixtime(F.unix_timestamp('AESTTime', 'yyyy-MM-dd  HH:mm:ss')))


'''
Fixing the timestamp column, 

The file  NMIG2.csv has timestamp format as dd/MM/yyyy HH:mm:ss while all other files has the format as yyyy-MM-dd HH:mm:ss
This makes the entire AESTTime column to be treated as a string. Therefore converting it to genreric timestamp type

'''

energy_data = energy_data.withColumn('aest_time',
                                     F.when(energy_data.AESTTime.rlike('[0-9]{4}-[0-9]{2}-[0-9]{2}\s+[0-9]{2}:[0-9]{2}:[0-9]{2}'),F.to_timestamp('AESTTime','yyyy-MM-dd HH:mm:ss'))
                                     .when(energy_data.AESTTime.rlike('[0-9]{2}\/[0-9]{2}\/[0-9]{4}\s+[0-9]{2}:[0-9]{2}:[0-9]{2}'),F.to_timestamp('AESTTime','dd/MM/yyyy HH:mm:ss')))
corrupt_data = corrupt_data.withColumn('aest_time',
                                     F.when(corrupt_data.AESTTime.rlike('[0-9]{4}-[0-9]{2}-[0-9]{2}\s+[0-9]{2}:[0-9]{2}:[0-9]{2}'),F.to_timestamp('AESTTime','yyyy-MM-dd HH:mm:ss'))
                                     .when(corrupt_data.AESTTime.rlike('[0-9]{2}\/[0-9]{2}\/[0-9]{4}\s+[0-9]{2}:[0-9]{2}:[0-9]{2}'),F.to_timestamp('AESTTime','dd/MM/yyyy HH:mm:ss')))
corrupt_data = corrupt_data.fillna(method='ffill', subset=['timestamp'])


# energy data transformations


filename = F.udf(lambda x: x.rsplit('/',-1)[-1].split('.')[0])

energy_data = energy_data \
        .withColumn('nmi_id', filename(energy_data.source_file_name))\
        .withColumn('quantity_kwh', F.when (energy_data.Unit =='Mwh', energy_data.Quantity * 1000).otherwise(energy_data.Quantity))\
        .withColumn('new_Unit', F.when (energy_data.Unit =='Mwh', 'Kwh').otherwise(energy_data.Unit))\
        .withColumn('hour', F.hour(energy_data['aest_time']))\
        .drop('Quantity','Unit')

energy_data = energy_data.join(meta_data, energy_data.nmi_id == meta_data.Nmi, "left")

hourly_median = energy_data.groupBy(F.col('hour'), F.col('nmi_id'), F.col('State')).agg(F.median('quantity_kwh').alias('median_energy_consumption_per_hour'))

# import pandas as pd
# import numpy as np
# import matplotlib.pyplot as plt
# energy_data = pd.read_csv('/Users/srivatsanprakash/Downloads/Data Engineering Test/Data/ConsumptionData/NMIA1.csv', parse_dates=['AESTTime'])
# energy_data.set_index('AESTTime', inplace=True)
# energy_data['new_Quantity'] = np.where(energy_data['Unit']=='Mwh', energy_data['Quantity']*1000,  energy_data['Quantity'])
# energy_data['new_Unit'] = np.where(energy_data['Unit']=='Mwh', 'kwh',  energy_data['Unit'])


# # energy_data = energy_data.resample('15T').mean()
# hourly_data = energy_data.groupby(energy_data.index.hour)


# hourly_mean = energy_data.resample('H').mean(numeric_only = True)
# hourly_mean.plot()