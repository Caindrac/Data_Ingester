from abc import ABC, abstractmethod
import pymongo
import pandas as pd
import glob

class Data_Ingester(ABC):
    def __init__(self):
        super().__init__()
    
    @abstractmethod
    def ingest_data(self, data_frame):
        pass
    
class Mongo_Data_Ingester(Data_Ingester):
    def __init__(self, mongo_address, mongo_port, db_name, collection_name):
        super().__init__()
        self.mongo_client = pymongo.MongoClient(mongo_address, mongo_port)
        self.db_name = db_name
        self.collection_name = collection_name
        #Data_Ingester
        
class Mongo_Csv_Pandas_Data_Ingester(Mongo_Data_Ingester):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
    def create_data_frame(self, data_folder: str):
        df = pd.concat([pd.read_csv(f) for f in glob.glob(data_folder)], ignore_index = True)
        df.rename(columns=lambda x: x.replace(".", "_"), inplace=True)
        return df
        
    def ingest_data(self, data_frame):
        mongo_db = self.mongo_client[self.db_name]
        db_collection = mongo_db[self.collection_name]
        records = data_frame.to_dict(orient = 'records')
        db_collection.insert_many(records)
        
        
class Mongo_Dask_Data_Ingester(Mongo_Data_Ingester):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
    def __load_data(self, data_frame):
        mongo_db = self.mongo_client[self.db_name]
        db_collection = mongo_db[self.collection_name]
        
        new_df = data_frame.rename(columns=lambda x: x.replace(".", "_"), inplace=False)
        records = new_df.to_dict(orient = 'records')
        db_collection.insert_many(records)
        return data_frame
        
    def ingest_data(self, data_frame):
        data_frame.map_partitions(self.__load_data).compute()
    