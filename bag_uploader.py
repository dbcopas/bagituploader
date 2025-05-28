import os
import hashlib
import json
import logging
import time
import threading
import signal
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import AzureError
from tqdm import tqdm
import requests


"""This script uploads files from a local directory to Azure Data Lake Storage (ADLS) Gen2.
It computes MD5 hashes for verification, handles retries, and logs progress.        
It supports graceful shutdown on interrupt signals and uses multithreading for parallel uploads.

invoke with:
python bag_uploader.py <local_directory> <storage_account_name> <filesystem_name> [--threads <num_threads>]

where:
- <local_directory> is the path to the local directory to upload.
- <storage_account_name> is the name of the Azure Storage account.
- <filesystem_name> is the name of the ADLS Gen2 filesystem (container).
- [--threads <num_threads>] is an optional argument to specify the number of parallel upload threads (default is 8).
It requires the Azure SDK for Python and tqdm for progress bars.
This script is designed to be run in a Python environment with the necessary Azure SDK packages installed.
It will log its operations to 'upload.log' and 'quick.log', and maintain a record of uploaded files in 'uploaded_files.json'.
This script is intended for use in environments where you need to upload large datasets to Azure Data Lake Storage efficiently,
and it includes features for error handling, progress tracking, and MD5 verification to ensure data integrity.

This script is provided as-is and should be tested in a safe environment before use in production.
"""


# Set up logging
logging.basicConfig(filename='upload.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
quicklog = logging.getLogger("quick")
quicklog.setLevel(logging.INFO)
fh = logging.FileHandler("quick.log")
fh.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
quicklog.addHandler(fh)
logging.info("Script started")

# Load config
with open("config.json") as f:
    config = json.load(f)
IGNORE_EXTENSIONS = set(config.get("ignore_extensions", []))
UPLOAD_LOG_PATH = "uploaded_files.json"
UPLOAD_LOG_LOCK = threading.Lock()
SHUTDOWN_REQUESTED = False

# Load or initialize upload log
if os.path.exists(UPLOAD_LOG_PATH):
    with open(UPLOAD_LOG_PATH, 'r') as f:
        uploaded_files = set(json.load(f))
    logging.info(f"Loaded {len(uploaded_files)} previously uploaded files from log")
else:
    uploaded_files = set()
    logging.info("No existing upload log found; starting fresh")

# Save uploaded file log safely
def save_upload_log():
    logging.info("Preparing to write uploaded_files log")
    try:
        with UPLOAD_LOG_LOCK:
            log_snapshot = list(uploaded_files)
        temp_path = UPLOAD_LOG_PATH + ".tmp"
        with open(temp_path, 'w') as f:
            json.dump(log_snapshot, f)
        os.replace(temp_path, UPLOAD_LOG_PATH)
        logging.info(f"Saved upload log with {len(log_snapshot)} entries")
    except Exception as e:
        logging.error(f"Failed to write upload log: {e}")

# Signal handler to allow graceful shutdown
def handle_sigint(signum, frame):
    global SHUTDOWN_REQUESTED
    print("\nInterrupt received, attempting to gracefully exit...")
    logging.warning("SIGINT received; setting shutdown flag")
    SHUTDOWN_REQUESTED = True

signal.signal(signal.SIGINT, handle_sigint)

# Helper to compute MD5 hash
def compute_md5(file_path):
    logging.debug(f"Computing MD5 for {file_path}")
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

# Helper to determine if a file should be ignored
def should_ignore(file_path):
    name = os.path.basename(file_path)
    if name.startswith('.') or name.startswith('~'):
        return True
    ext = os.path.splitext(name)[1].lower()
    return ext in IGNORE_EXTENSIONS

# Fallback MD5 download verifier using blob URL and requests
def verify_remote_md5(file_client, md5_local, file_path):
    retries = 3
    while retries > 0:
        try:
            logging.info(f"Verifying MD5 using fallback for: {file_path}")
            url = file_client.url.replace(".dfs.", ".blob.")
            response = requests.get(url, stream=True, timeout=10)
            response.raise_for_status()
            md5_remote = hashlib.md5()
            for chunk in response.iter_content(chunk_size=8192):
                md5_remote.update(chunk)
            md5_final = md5_remote.hexdigest()
            quicklog.info(f"Fallback: downloaded file {file_path}. Computed MD5: {md5_final}. Expected: {md5_local}. Match: {md5_final == md5_local}")
            if md5_local != md5_final:
                raise ValueError("MD5 mismatch via fallback")
            return True
        except Exception as e:
            retries -= 1
            logging.warning(f"Fallback MD5 check failed: {e}. Retries left: {retries}")
            time.sleep(2)
    raise Exception(f"Remote MD5 verification failed for {file_path} after retries")

# Upload a single file and verify MD5
def upload_and_verify_file(file_path, file_client, remote_file_path, progress, upload_results, flush_counter):
    global SHUTDOWN_REQUESTED
    if SHUTDOWN_REQUESTED:
        return

    if remote_file_path in uploaded_files:
        logging.info(f"Skipping already-uploaded file: {file_path}")
        progress.update(1)
        return

    retries = 3
    while retries > 0 and not SHUTDOWN_REQUESTED:
        try:
            logging.info(f"Uploading: {file_path} -> {remote_file_path}")
            md5_local = compute_md5(file_path)
            quicklog.info(f"Computed MD5 of {file_path} is {md5_local}")
            with open(file_path, 'rb') as data:
                file_client.upload_data(data, overwrite=True)
            logging.info(f"Upload complete: {file_path}, starting download for verification")
            quicklog.info(f"Uploaded file {file_path}")

            try:
                stream = file_client.download_file()
                md5_remote = hashlib.md5()
                for chunk in stream.chunks():
                    md5_remote.update(chunk)
                md5_remote_final = md5_remote.hexdigest()
                quicklog.info(f"Downloaded file {file_path}. Computed MD5: {md5_remote_final}. Expected: {md5_local}. Match: {md5_local == md5_remote_final}")
                if md5_local != md5_remote_final:
                    raise ValueError("MD5 mismatch")
            except Exception as stream_error:
                logging.warning(f"Streaming MD5 failed for {file_path}, trying fallback method: {stream_error}")
                verify_remote_md5(file_client, md5_local, file_path)

            with UPLOAD_LOG_LOCK:
                upload_results.append(remote_file_path)
                flush_counter[0] += 1
                if flush_counter[0] % 10 == 0:
                    uploaded_files.update(upload_results)
            if flush_counter[0] % 10 == 0:
                save_upload_log()

            logging.info(f"Uploaded and verified: {file_path}")
            progress.update(1)
            return
        except Exception as e:
            retries -= 1
            logging.error(f"Error uploading {file_path}: {str(e)}. Retries left: {retries}")
            time.sleep(2)
# Worker wrapper
def upload_worker(args):
    file_path, file_client, remote_file_path, progress, upload_results, flush_counter = args
    upload_and_verify_file(file_path, file_client, remote_file_path, progress, upload_results, flush_counter)

# Recursively upload directory
def upload_directory(local_path, filesystem_client, remote_path="", max_workers=8):
    logging.info(f"Starting upload from local path: {local_path}")
    upload_tasks = []
    upload_results = []
    flush_counter = [0]

    for root, dirs, files in os.walk(local_path):
        rel_path = os.path.relpath(root, local_path)
        remote_dir = os.path.join(remote_path, rel_path).replace("\\", "/") if rel_path != '.' else remote_path
        if remote_dir:
            logging.info(f"Creating remote directory: {remote_dir}")
            filesystem_client.get_directory_client(remote_dir).create_directory()
        for file in files:
            file_path = os.path.join(root, file)
            if should_ignore(file_path):
                logging.info(f"Ignored: {file_path}")
                continue
            remote_file_path = os.path.join(remote_dir, file).replace("\\", "/")
            file_client = filesystem_client.get_file_client(remote_file_path)
            upload_tasks.append((file_path, file_client, remote_file_path))

    logging.info(f"Queued {len(upload_tasks)} files for upload")

    with tqdm(total=len(upload_tasks), desc="Uploading files") as progress:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(upload_worker, (task[0], task[1], task[2], progress, upload_results, flush_counter)) for task in upload_tasks]
            try:
                for future in as_completed(futures):
                    if SHUTDOWN_REQUESTED:
                        logging.warning("Shutdown requested. Breaking upload loop.")
                        break
                    future.result()
            except KeyboardInterrupt:
                print("Interrupted during upload execution")
                logging.warning("Upload interrupted by user")

    # Final write
    uploaded_files.update(upload_results)
    save_upload_log()

# Main function
def main(local_dir, storage_account_name, filesystem_name, max_workers):
    try:
        logging.info(f"Initializing DataLakeServiceClient for account: {storage_account_name}")
        credential = DefaultAzureCredential()
        service_client = DataLakeServiceClient(account_url=f"https://{storage_account_name}.dfs.core.windows.net", credential=credential)
        filesystem_client = service_client.get_file_system_client(filesystem_name)
        upload_directory(local_dir, filesystem_client, max_workers=max_workers)
    except AzureError as e:
        logging.critical(f"Azure Error: {str(e)}")
    except Exception as e:
        logging.critical(f"Unexpected Error: {str(e)}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Upload a local directory to ADLS Gen2")
    parser.add_argument("local_dir", help="Local directory to upload")
    parser.add_argument("storage_account", help="Azure Storage account name")
    parser.add_argument("filesystem", help="ADLS Gen2 filesystem (container) name")
    parser.add_argument("--threads", type=int, default=8, help="Number of parallel upload threads (default: 8)")
    args = parser.parse_args()
    main(args.local_dir, args.storage_account, args.filesystem, max_workers=args.threads)
