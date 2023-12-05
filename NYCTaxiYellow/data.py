# Pip install packages
# !{sys.executable} -m pip install azure-storage-blob
# !{sys.executable} -m pip install pyarrow
# !{sys.executable} -m pip install pandas

import os, sys
import pandas as pd
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

class YellowTaxi():
    def __init__(self, start_date, end_date):
        self.azure_storage_account_name = "azureopendatastorage"
        self.azure_storage_sas_token = r""
        self.container_name = "nyctlc"
        self.folder_name = "yellow"
        self.start_date=start_date
        self.end_date=end_date

    def months_and_dates_between(self,start_date, end_date):
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        current_date = start
        all_dates = []
        while current_date <= end:
            all_dates.append(current_date.strftime('%Y-%m'))  # Formatting for YYYY-M
            current_date += timedelta(days=1)
        unique_months = list(set(all_dates))
        return unique_months

    def access_data(self):
        print('Looking for the first parquet under the folder ' + self.folder_name + ' in container "' + self.container_name + '"...')
        container_url = f"https://{self.azure_storage_account_name}.blob.core.windows.net/"
        blob_service_client = BlobServiceClient(
                                    container_url,
                                    self.azure_storage_sas_token if self.azure_storage_sas_token else None
                                )
        container_client = blob_service_client.get_container_client(self.container_name)
        blobs = container_client.list_blobs(self.folder_name)
        return blobs, container_client

    def get_data(self):
        blobs, container_client=self.access_data()
        dates_to_check=self.months_and_dates_between(self.start_date,self.end_date)
        targetBlobName = []
        for blob in blobs:
            if blob.name.startswith(self.folder_name) and blob.name.endswith('.parquet'):
                partition_date=str(blob).split('/')[1].replace('puYear=','')+'-'+str(blob).split('/')[2].replace('puMonth=','')
                if partition_date in dates_to_check:
                    targetBlobName.append(blob.name)

        for file_name in targetBlobName:
            _, filename = os.path.split(str(file_name))
            blob_client = container_client.get_blob_client(file_name)
            with open(filename, 'wb') as local_file:
                try:
                    blob_client.download_blob().download_to_stream(local_file)
                except Exception as e:
                    print(e)

object_=YellowTaxi('2018-05-1','2018-07-1')
object_.get_data()