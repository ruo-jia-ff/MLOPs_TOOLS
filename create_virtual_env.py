import os
import subprocess
import sys
import zipfile
from pathlib import Path

def delete_empty_folders(root_dir):
    for dirpath, dirnames, filenames in os.walk(root_dir, topdown=False):
        if not dirnames and not filenames:
            try:
                os.rmdir(dirpath)
                print(f"Deleted empty folder: {dirpath}")
            except OSError as e:
                print(f"Failed to delete {dirpath}: {e}")

def unzip_and_delete(folder_path = os.getcwd()):
    zip_files = []
    for filename in os.listdir(folder_path):
        if filename.lower().endswith('.zip'):
            zip_path = os.path.join(folder_path, filename)
            extract_dir = os.path.join(folder_path, os.path.splitext(filename)[0])
            zip_files.append(zip_path)

            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_dir)
                print(f"Unzipped: {filename}")
            except zipfile.BadZipFile:
                print(f"Bad zip file: {filename}")
            except Exception as e:
                print(f"Error processing {filename}: {e}")

    for zip_file in zip_files:
        try:
            os.remove(zip_file)
            print(f"Deleted: {zip_file}")
        except FileNotFoundError:
            print(f"{zip_file} not found...")
        except Exception as e:
            print(f"Error with {zip_file}: e")

def setup_venv(venv_dir='venv', requirements_file='requirements.txt'):
    """
    Creates a virtual environment and installs packages from requirements.txt.
    
    Parameters:
    - venv_dir (str): Name of the virtual environment directory.
    - requirements_file (str): Path to the requirements.txt file.
    """
    venv_path = Path(venv_dir)
    req_path = Path(requirements_file)

    print(f"Creating venv: {venv_path}")

    if not req_path.exists():
        raise FileNotFoundError(f"Requirements file '{requirements_file}' not found.")

    # Create virtual environment
    subprocess.check_call([sys.executable, '-m', 'venv', str(venv_path)])
    print(f"Virtual environment created at: {venv_path}")

    # Determine pip path in the venv
    if os.name == 'nt':  # Windows
        pip_path = venv_path / 'Scripts' / 'pip.exe'
    else:  # Unix/Linux/macOS
        pip_path = venv_path / 'bin' / 'pip'

    # Install from requirements.txt
    subprocess.check_call([str(pip_path), 'install', '-r', str(req_path)])
    print(f"Installed packages from '{requirements_file}'")

if __name__ == "__main__":
    venv_name = "venv_" + Path(os.getcwd()).stem.lower()
    requirements = 'requirements.txt'

    try:
        delete_empty_folders(".")
        unzip_and_delete()
        setup_venv(venv_name, requirements)
        print(f"::venv_name::{venv_name}")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)