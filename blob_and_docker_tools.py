from azure_blob_user import create_blob_storage_client_via_key, AzureBlobUser
import subprocess
from time import sleep
import shutil
import os
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(".env.data")
env_folder = os.getenv("ENV_FOLDER")
image_folder = os.getenv("IMAGE_FOLDER")

# Set up the blob_user
blob_storage_client, expiry = create_blob_storage_client_via_key()
blob_user = AzureBlobUser(blob_storage_client, expiry)

# Set where the files are going
destination_folder = os.path.join(os.getcwd(), "docker_image_folder_temp")

def run_command(command):
    result = subprocess.run(
        command,
        encoding="utf-8",
        capture_output=True,
        text=True,
        check=True
    )
    print(result.stdout)
    return result

def load_image_to_docker(docker_image_path):
    result = run_command(["docker", "load", "-i", docker_image_path])
    loaded_line = [l for l in result.stdout.splitlines() if "Loaded image" in l][0]
    print("Docker image loaded...")
    return loaded_line.split("Loaded image: ")[-1].strip()

def run_docker_image_from_image_path(docker_image):

    reset_temp_folder(destination_folder)
    print("Temp folder cleared...")
    
    print(f"Running docker image: {docker_image}")

    command = [
    "docker", "run", "--rm",
    "-v", f"{env_folder}:/app/env_folder",
    "-v", f"{image_folder}:/app/data",
    docker_image
    ]

    try:
        run_command(command)

    except subprocess.CalledProcessError as e:
        print(f"Container failed: {docker_image}")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        raise

    finally:
        subprocess.run(["docker", "rmi", "-f", docker_image], check=False)

def reset_temp_folder(folder_path: str):

    folder = Path(folder_path).resolve()

    if str(folder) in ["/", "/tmp"]:
        raise ValueError("Refusing to delete critical directory")

    if folder.exists():
        shutil.rmtree(folder)

    folder.mkdir(parents=True, exist_ok=True)

def acquire_blob_given_blobpath(sample_blob_path,
                                destination_folder = destination_folder,
                                blob_user = blob_user,
                                number_of_tries = 3,
                                verbose = True):
    
    blob_path = sample_blob_path.split("//")[1]
    _, blob_container, blob_name = blob_path.split("/")

    downloaded = False
    os.makedirs(destination_folder, exist_ok=True)
    destination_path = os.path.join(destination_folder, blob_name)

    blob_user.establish_blob_container(blob_container)

    if verbose:
        print(f"Attempting download of {blob_name} to {destination_path}")

    for i in range(number_of_tries):
        try:
            blob_user.establish_blob_client(blob_name)
            blob_user.download_blob_client_contents(destination_path)
            downloaded = True
            break
        except:
            print(f"Download of {blob_name} failed... trying again. Number of tries is {i+1}")
            sleep(30)

    if not downloaded:
        raise RuntimeError(f"Failed to download {blob_name} after {number_of_tries} attempts.")

    if downloaded and verbose:
        print(f"Downloaded {blob_name} to {destination_path}!!")

    return destination_path

def run_downloaded_blob_via_docker(destination_path):
    print("Loading image into docker...")
    docker_image = load_image_to_docker(destination_path)
    run_docker_image_from_image_path(docker_image)
    print("Docker container completed it's run...")