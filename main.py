from Ingesters.ingester import Mongo_Csv_Pandas_Data_Ingester, Mongo_Dask_Data_Ingester
import dask.dataframe as dd
import argparse
import json



def csv_example(mongo_address, mongo_port, db_name, collection_name, csv_location):
    ingester = Mongo_Csv_Pandas_Data_Ingester(mongo_address=mongo_address, mongo_port=mongo_port, db_name=db_name, collection_name=collection_name)
    # ingester.ingest_data(data_frame=ingester.create_data_frame(data_folder="data/table_P3/sumLevel_140/*.csv"))
    ingester.ingest_data(data_frame=ingester.create_data_frame(data_folder=csv_location))


def dask_example(mongo_address, mongo_port, db_name, collection_name, csv_location):
    # ingester = Mongo_Dask_Data_Ingester(mongo_address="127.0.0.1", mongo_port=27017, db_name="Data_Ingester", collection_name="Table_P3-sumLevel_140")
    ingester = Mongo_Dask_Data_Ingester(mongo_address=mongo_address, mongo_port=mongo_port, db_name=db_name, collection_name=collection_name)
    # df = dd.read_csv("data/table_P3/sumLevel_140/*.csv")
    df = dd.read_csv(csv_location)
    ingester.ingest_data(data_frame=df)

def read_config(config_file_location: str):
    with open(config_file_location) as config_file:
        return json.load(config_file)

if __name__ == "__main__":
    class_mapper = {
        "dask_example": dask_example,
        "csv_example": csv_example
    }
    parser = argparse.ArgumentParser(description="Tool to mass donwlaod data from http://census.ire.org/data/bulkdata.html")
    parser.add_argument("--config_file", type=str, required=True)
    args = parser.parse_args()
    
    config = read_config(config_file_location=args.config_file)
    
    for download in config['example']:
        download_instance = class_mapper[download['function']]
        download_instance(**download['function_variables'])