from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, generate_account_sas, ResourceTypes, AccountSasPermissions
from azure.core.exceptions import AzureError
from datetime import datetime, timedelta, timezone

import os 
from dotenv import load_dotenv

load_dotenv(".env.blob")
STORAGE_ACCOUNT = os.getenv('STORAGE_ACCOUNT')
ACCESS_KEY = os.getenv('ACCESS_KEY')

def create_blob_storage_client_via_key(hours_of_use: int = 72):

    expiry = datetime.now(timezone.utc) + timedelta(hours=hours_of_use)

    sas_token = generate_account_sas(
        account_name=STORAGE_ACCOUNT,
        account_key=ACCESS_KEY,
        resource_types=ResourceTypes(service=True, container=True, object=True),
        permission=AccountSasPermissions(list=True, create=True, read = True, write=True, delete=True),
        expiry=expiry
    )

    account_url = f"https://{STORAGE_ACCOUNT}.blob.core.windows.net"
    sas_url = f"{account_url}?{sas_token}"

    return BlobServiceClient(account_url=sas_url), expiry

class AzureBlobUser:
    def __init__(self, 
                 blob_service_client: BlobServiceClient,
                 expiry: datetime):
        
        self.blob_service_client = blob_service_client
        self.container_client: None|ContainerClient = None 
        self.blob_service_expiry: datetime = expiry
        self.blob_client: None|BlobClient = None 

    def check_if_client_needs_reset(self):

        if self.blob_service_expiry - datetime.now(timezone.utc) < timedelta(hours=2):

            blob_client, expiry = create_blob_storage_client_via_key()
            
            self.blob_service_client = blob_client
            self.blob_service_expiry = expiry

    def list_containers(self, return_containers = True):

        if self.blob_service_client is None:
            return "No blob service client available..."

        try:
            # List containers
            containers = []
            for container in self.blob_service_client.list_containers():
                containers.append(container)

            print("Containers:")
            container_list = "\n".join(["-" + c['name'] for c in containers])
            print(container_list)
            
            if return_containers:
                return containers
                
        except Exception as e:
            print(f"Error listing containers: {e}")
            raise

    def create_container(self,
                         container_name,
                         set_as_current_container = True):
        
        try:
            container_client = self.blob_service_client.create_container(container_name)
            print(f"Container '{container_name}' created.")

            if set_as_current_container:
                self.container_client = container_client
                
        except Exception as e:
            print(f"Could not create container: {e}")

    def establish_blob_container(self,
                                 container_name,
                                 list_blobs = False):
        
        if self.blob_service_client is None:
            return "No blob service client available..."
        
        # Set the container 
        container_client = self.blob_service_client.get_container_client(container_name)
        self.container_client = container_client
        
        if list_blobs:
            print(f"Blobs in container {container_name}:")
            for blob in container_client.list_blobs():
                print(f"- {blob.name}")

    def list_blobs_in_container(self,
                                container = None,
                                name_starts_with = None,
                                verbose = True,
                                return_blobs = False):
        
        if container is None: 
            container = self.container_client
            
        if container is None:
            print("No container available...")
            return None   

        if name_starts_with:
            blobs = [b for b in container.list_blobs(name_starts_with=name_starts_with)]
        else:
            blobs = [b for b in container.list_blobs()]

        if verbose:
            print(f"Container {container.container_name} contains:")
            for blob in blobs:
                print(blob.name)
        
        if return_blobs:
            return blobs
        
    def get_blobs_in_container(self, *args, **kwargs): # Alias for list blobs in container
        return self.list_blobs_in_container(*args, **kwargs)

    def establish_blob_client(self,
                              blob_name = None,
                              container = None,
                              verbose = False):
        
        if container is None: 
           container = self.container_client
            
        if container is None:
            print("No container available...")
            return None
        
        # Reset the self.container
        self.container_client = container 
        
        try:
            blob_client = self.blob_service_client.get_blob_client(container=container.container_name, 
                                                                   blob=blob_name)
            
            if not blob_client.exists():
                print(f"Blob client {blob_name} not in {container.container_name}... New client established...") 
            
            self.blob_client = blob_client

            if verbose:
                print(f"Blob client set for {blob_name} in {container.container_name}")
            
        except AzureError as e:
            print(f"BlobClient '{blob_name}' in container '{container.container_name}': {e}")
      
    def download_blob_client_contents(self,
                                      download_path = None,
                                      chunk_size = 10 * 1024 * 1024):
        
        if self.blob_client is None:
            print("No blob client set...")
            return None  

        if download_path is None:
            download_path = os.path.join(os.getcwd(), self.blob_client.blob_name)

        try:
            # Download the file and read it in as chunks
            stream = self.blob_client.download_blob()    
            with open(download_path, "wb") as file:
                while True:
                    chunk = stream.read(chunk_size)                    
                    
                    if not chunk:
                        break 

                    file.write(chunk)
        
            return download_path

        except AzureError as e:
            print(f"Download failed... error: {e}")

    def upload_file_to_blob_container(self,
                                      local_file_path, 
                                      blob_name = None,
                                      container = None,):

        try:
            if container is None:
                container = self.container_client

            if container is None:
                print("No container to upload to...")
                return None 
            
            # Default to local file path's blob name 
            if blob_name is None:
                blob_name = os.path.basename(local_file_path)

            blob_client = self.container_client.get_blob_client(blob_name)

            # Upload the file to the blob storage
            with open(local_file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)  # Set overwrite=True to overwrite an existing blob with the same name

            print(f"File '{local_file_path}' uploaded to blob storage as '{blob_name}' in container '{container.container_name}'")
        
        except Exception as e:
            print(f"An error occurred, file could not be uploaded: {e}")

    def delete_blob(self,
                    blob_client = None,
                    verbose = True):

        if blob_client is None:
            blob_client = self.blob_client

        if blob_client is None:        
            print("No blob to delete...")

        self.blob_client.delete_blob()

        if verbose:
            print(f"{blob_client.blob_name} has been deleted from {blob_client.container_name}")

    def delete_all_container_files(self,
                                   container = None):
        
        if container is None:
            container = self.container_client

        if container is None:
            print("No container provided...")

        try:
            for blob in container.list_blobs():
                container.delete_blob(blob.name)
            print("All files in blob container deleted!")
    
        except Exception as e:
            print(f"An error deleting files in blob container!: {e}")

    def hello_world(self,
                    container_name = None,
                    clean_up = True):
        
        if self.blob_service_client is None:
            return "No blob service client available..."
        
        if not container_name:
            containers = self.list_containers(return_containers=True)

            if not containers:
                return "No containers in blob storage..."
            container_names = [c.name for c in containers]

        else:
            container_names = [container_name]

        for container_name in container_names:
            blob_client = self.blob_service_client.get_blob_client(container=container_name, blob="hello-world.txt")
            blob_client.upload_blob(f"Hello World! The container {container_name} works on {datetime.now().date()}", overwrite=True)

            blob_data = blob_client.download_blob()
            text_content = blob_data.readall().decode('utf-8')  # decode as needed...
            print(text_content)

            if clean_up:
                blob_client.delete_blob()