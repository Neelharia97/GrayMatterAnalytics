from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from itertools import chain
import os


class Summarize():
    def __init__(self,
                start_date=None,
                end_date=None):
        self.data=None
        self.master_list=[]
        self.start_date=start_date
        self.end_date=end_date
        self.pwd=os.getcwd()
        self.spark=SparkSession \
                    .builder \
                    .appName("Gray Matter Analytics") \
                    .getOrCreate()

    def create_dataset(self):
        self.master_list = []
        for filename in os.listdir(self.pwd):
            _,extension=os.path.splitext(filename)
            if extension=='.parquet':
                self.master_list.append(filename)
        self.data=self.spark.read.parquet(*self.master_list)
    
    def cleanUp(self):
        for filename in self.master_list:
            os.remove(f"{self.pwd}/{filename}")

    def transformation(self):
        columns=['paymentType',
                 'totalAmount',
                 'tipAmount',
                 'tollsAmount',
                 'fareAmount',
                 'extra',
                 'mtaTax',
                 'improvementSurcharge',
                 'tpepPickupDateTime',
                 'tpepDropoffDateTime',
                 'passengerCount',
                 'tripDistance',
                 'rateCodeId',
                 'vendorID'
                 ]
        subset=self.data.select(*columns)
        subset=subset.filter(
            (to_date(col('tpepPickupDateTime'))>=self.start_date) & (to_date(col('tpepPickupDateTime'))<=self.end_date)
        )
        conditions = {
            "paymentType" : {
                "1":"Credit card",
                "2": "Cash",
                "3": "No charge",
                "4": "Dispute" ,
                "5": "Unknown",
                "6": "Voided trip"
            },
            "rateCodeId" : {
                "1": "Standard rate",
                "2": "JFK",
                "3": "Newark",
                "4": "Nassau or Westchester",
                "5": "Negotiated fare",
                "6": "Group ride"
            }
        }

        for col_name in conditions.keys():
            mapping_expr = create_map([lit(x) for x in chain(*conditions[col_name].items())])
            subset=subset.withColumn(f"{col_name}Code", mapping_expr[col(col_name)])
        subset=subset.withColumn ("TripDurationMinutes",round((unix_timestamp(col("tpepDropoffDateTime")) - unix_timestamp(col("tpepPickupDateTime"))) / 60))
        subset=subset.withColumn('year',year(col('tpepPickupDateTime'))).withColumn('month',month(col('tpepPickupDateTime')))
        subset=subset.drop('paymentType','rateCodeId')
        return subset

    def primary_analysis(self,df):
        print("Generating primary analysis...")
        df=df.groupBy(
            'paymentTypeCode',
            'year',
            'month'
        ).agg(
            round(mean('fareAmount'),2).alias('meanFareAmount'),
            round(mean('totalAmount'),2).alias('meanTotalAmount'),
            round(mean('passengerCount')).alias('meanPassengerCount'),
            round(median('fareAmount'),2).alias('medianFareAmount'),
            round(median('totalAmount'),2).alias('medianTotalAmount'),
            round(median('passengerCount')).alias('medianPassengerCount'),
            sum('passengerCount').alias('TotalPassengers')
        ).orderBy(
            'month',
            'year'
        )
        return df

    def secondary_analysis(self, df):
        print("Generating secondary analysis....")
        df=df.groupBy(
            'paymentTypeCode',
            'rateCodeIdCode',
            'year',
            'month'
        ).agg(
            count('paymentTypeCode').alias('TotalTrips'),
            round(sum('totalAmount'),2).alias('totalRevenueByPayment'),
            round(sum('tipAmount'),2).alias('totalTipsCollected'),
            round(sum('tollsAmount'),2).alias('totalTollsPaid')
        ).orderBy(
            'month',
            'year'
        )
        return df

    def generate_report(self, tabs=[]):
        for sheet in tabs:
            sheet[0].write.mode('overwrite').csv(f"{self.pwd}/{sheet[1]}",header=True)

    def analysis(self):
        data=self.transformation()
        distinct_payments=data.select('paymentTypeCode').distinct()
        print(f"The number of distinct payment types are : {distinct_payments.count()} \n")

        average_trip_duration=data.select(avg('TripDurationMinutes')).collect()[0][0]
        print(f"Average Trip Duration in Minutes: {average_trip_duration} \n")

        print(f"Total trips including voided and non-chargeable transactions: {data.count()} \n")    

        invalid_trips=data.filter(col('paymentTypeCode').isin('Dispute','Voided trip'))
        print(f"Total disputed or invalid-trips: {invalid_trips.count()} \n")

        valid_trips=data.filter(col('paymentTypeCode').isin('Cash','Credit Card'))
        print(f"Total trips paid by Cash/Credit {valid_trips.count()} \n")

        total_analyzed=self.primary_analysis(data)
        valid_analyzed=self.primary_analysis(valid_trips)
        invalid_analyzed=self.primary_analysis(invalid_trips)

        self.generate_report([
            [total_analyzed,'total_analyzed/total_primary.csv'],
            [valid_analyzed,'valid_analyzed/valid_primary.csv'],
            [invalid_analyzed,'invalid_analyzed/invalid_primary.csv']
        ])

        total_analyzed=self.secondary_analysis(data)
        valid_analyzed=self.secondary_analysis(valid_trips)
        invalid_analyzed=self.secondary_analysis(invalid_trips)

        self.generate_report([
            [total_analyzed,'total_analyzed/total_secondary.csv'],
            [valid_analyzed,'valid_analyzed/valid_secondary.csv'],
            [invalid_analyzed,'invalid_analyzed/invalid_secondary.csv']
        ])
