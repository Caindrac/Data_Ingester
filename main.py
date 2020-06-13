from Ingesters.ingester import Mongo_Csv_Pandas_Data_Ingester, Mongo_Dask_Data_Ingester
import dask.dataframe as dd


# ingester = Mongo_Csv_Pandas_Data_Ingester(mongo_address="127.0.0.1", mongo_port=27017, db_name="Data_Ingester", collection_name="Table_P3-sumLevel_140")

# ingester.ingest_data(data_frame=ingester.create_data_frame(data_folder="data/table_P3/sumLevel_140/*.csv"))


ingester = Mongo_Dask_Data_Ingester(mongo_address="127.0.0.1", mongo_port=27017, db_name="Data_Ingester", collection_name="Table_P3-sumLevel_140")
df = dd.read_csv("data/table_P3/sumLevel_140/*.csv")
# print(df.head(10))
ingester.ingest_data(data_frame=df)