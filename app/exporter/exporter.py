import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import http.client
import argparse
import subprocess
import hashlib
import mimetypes
from datetime import datetime

# Constants
CHUNK_SIZE = 8192
LOG_LEVEL_INFO = "INFO"
LOG_LEVEL_ERROR = "ERROR"
max_workers = 16

class Log:
    def __init__(self, endpoint_url):
        """
        Initialize the Log class.
        Args:
            endpoint_url (str): The endpoint URL for sending logs.
        """
        self.endpoint_url = endpoint_url.replace("http://", "").replace("https://", "")

    def send_log_message(self, level, message):
        """
        Sends a log message to the specified endpoint.
        Args:
            level (str): The log level (e.g., "INFO", "ERROR").
            message (str): The log message to send.
        Returns:
            str: The response from the log server.
        """
        conn = http.client.HTTPConnection(self.endpoint_url)
        payload = json.dumps({"level": level, "message": message})
        headers = {'Content-Type': 'application/json'}
        conn.request("POST", "/log", payload, headers)
        response = conn.getresponse()
        data = response.read()
        conn.close()  # Ensure the connection is closed
        return data

    def info(self, message):
        """
        Logs an informational message.
        Args:
            message (str): The message to log.
        """
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        self.send_log_message(LOG_LEVEL_INFO, f"{timestamp} - INFO - {message}")

    def error(self, message):
        """
        Logs an error message.
        Args:
            message (str): The message to log.
        """
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        self.send_log_message(LOG_LEVEL_ERROR, f"{timestamp} - ERROR - {message}")
# Initialize global logging variable
log = None


def load_config():
    """
    Loads the configuration from the 'config.json' file.

    Returns:
        dict: The configuration dictionary.
    """
    with open('config.json', 'r') as config_file:
        return json.load(config_file)


def save_config(config):
    """
    Saves the configuration into the 'config.json' file.

    Args:
        config (dict): The configuration dictionary to save.
    """
    with open('config.json', 'w') as config_file:
        json.dump(config, config_file, indent=4)

def generate_file_id(file_path):
    """
    Generate a unique identifier for a file based on its path and content.
    Args:
        file_path (str): The path to the file.
    Returns:
        str: The SHA-256 hash-based file identifier, or None if the file is missing.
    """
    sha256 = hashlib.sha256()
    sha256.update(file_path.encode('utf-8'))
    try:
        with open(file_path, 'rb') as f:
            while chunk := f.read(CHUNK_SIZE):
                sha256.update(chunk)
    except FileNotFoundError:
        log.error(f"File {file_path} not found for hashing.")
        return None
    return sha256.hexdigest()


def get_file_properties(file_path, dataset):
    """
    Gather metadata for a given file, including its size, MIME type, and extension.
    Args:
        file_path (str): The path to the file.
    Returns:
        dict: File metadata including `path`, `file_id`, `filename`, `extension`, `size_bytes`,
              `size_human`, and `mime_type`. Returns None if metadata cannot be collected.
    """
    try:
        file_id = generate_file_id(file_path)
        if file_id is None:
            return None
        file_info = {
            "path": file_path,
            "file_id": file_id,
            "filename": os.path.basename(file_path),
            "extension": os.path.splitext(file_path)[1],
            "size_bytes": os.path.getsize(file_path),
            "size_human": human_readable_size(os.path.getsize(file_path)),
            "mime_type": mimetypes.guess_type(file_path)[0] or "unknown",
            "dataset": str(dataset)  # Including the dataset name
        }
        log.info(f"File properties collected: {file_info}")
        return file_info
    except (OSError, IOError) as e:
        log.error(f"Failed to get properties for file {file_path}: {e}")
        return None


def send_file_info_message(file_info, endpoint_url):
    """
    Sends file information to the specified endpoint.

    Args:
        file_info (dict): The file information to send.
        endpoint_url (str): The endpoint URL for sending data.

    Returns:
        str: The response from the server.
    """
    conn = http.client.HTTPConnection(endpoint_url.replace("http://", "").replace("https://", ""))
    payload = json.dumps(file_info)
    headers = {'Content-Type': 'application/json'}
    conn.request("POST", "/files", payload, headers)
    response = conn.getresponse()
    data = response.read()
    conn.close()
    return data


def get_snapshots(dataset_path):
    """
    Retrieves ZFS snapshots for a given dataset.

    Args:
        dataset_path (str): The ZFS dataset path.

    Returns:
        list: A list of snapshot names.
    """
    command = f"zfs list -t snapshot -o name -s creation -r {dataset_path}"
    result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        raise Exception(f"Error running command {command}: {result.stderr.decode()}")
    output = result.stdout.decode()
    snapshots = output.strip().split('\n')[1:]  # Skip the header line
    return snapshots


def parse_snapshot_timestamp(snapshot_name):
    """
    Parses the timestamp from a snapshot name.

    Args:
        snapshot_name (str): The snapshot name.

    Returns:
        datetime: The parsed timestamp.
    """
    snapshot_timestamp = snapshot_name.split('@')[1].replace("auto-", "")
    return datetime.strptime(snapshot_timestamp, "%Y-%m-%d_%H-%M")


def filter_snapshots_after(snapshots, timestamp):
    """
    Filters snapshots after a given timestamp.

    Args:
        snapshots (list): The list of snapshots.
        timestamp (datetime): The timestamp to filter against.

    Returns:
        list: A list of snapshots after the given timestamp.
    """
    return [s for s in snapshots if parse_snapshot_timestamp(s) > timestamp]

def process_file(file_path, dataset, endpoint_url):
    file_info = get_file_properties(file_path, dataset)
    if file_info:
        send_file_info_message(file_info, endpoint_url)
        return file_info
    return None

def process_file(file_path, endpoint_url):
    file_info = get_file_properties(file_path)
    if file_info:
        send_file_info_message(file_info, endpoint_url)
        return file_info
    return None


def scan_incremental(dataset_path, last_scan_timestamp, endpoint_url, file_extensions, max_workers, dataset_name):
    """
    Perform an incremental scan of the dataset from the nearest snapshot since the last scan.

    Args:
        dataset_path (str): The path to the dataset to be scanned.
        last_scan_timestamp (datetime): The timestamp of the last scan.
        endpoint_url (str): The endpoint URL to send file information messages.
        file_extensions (list): List of allowed file extensions.
        max_workers (int): Number of maximum worker threads for concurrent processing.

    Returns:
        list: A list of file information dictionaries for new or changed files.
    """
    snapshots = get_snapshots(dataset_path)
    if not snapshots:
        return []

    nearest_snapshot = min(
        snapshots,
        key=lambda s: abs(parse_snapshot_timestamp(s) - last_scan_timestamp)
    )
    newer_snapshots = filter_snapshots_after(snapshots, parse_snapshot_timestamp(nearest_snapshot))

    file_info_list = []
    futures = []



    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for snapshot in newer_snapshots:
            for root, _, files in os.walk(snapshot):
                for filename in files:
                    if any(filename.endswith(ext) for ext in file_extensions):
                        file_path = os.path.join(root, filename)
                        futures.append(executor.submit(process_file, file_path, dataset_name, endpoint_url))

        for future in as_completed(futures):
            result = future.result()
            if result:
                file_info_list.append(result)

    return file_info_list


def human_readable_size(size_in_bytes):
    """Convert a file size in bytes to a human-readable format (e.g., KB, MB).

    Args:
        size_in_bytes (int): File size in bytes.

    Returns:
        str: Human-readable file size (e.g., "4.00 KB").
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_in_bytes < 1024:
            return f"{size_in_bytes:.2f} {unit}"
        size_in_bytes /= 1024

def scan_full(dataset_path, endpoint_url, file_extensions, max_workers=max_workers, dataset_name):
    """
    Perform a full scan of the dataset.

    Args:
        dataset_path (str): The path to the dataset to be scanned.
        endpoint_url (str): The endpoint URL to send file information messages.
        file_extensions (list): List of allowed file extensions.
        max_workers (int): The maximum number of worker threads.

    Returns:
        list: A list of file information dictionaries for all files in the dataset.
    """
    file_info_list = []
    futures = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for root, _, files in os.walk(dataset_path):
            for filename in files:
                if any(filename.endswith(ext) for ext in file_extensions):
                    file_path = os.path.join(root, filename)
                    futures.append(executor.submit(process_file, file_path, dataset_name, endpoint_url))

        for future in as_completed(futures):
            result = future.result()
            if result:
                file_info_list.append(result)

    return file_info_list


def scan_datasets(config, endpoint_url, scan_type, file_extensions, dataset_name=None):
    """
    Scan datasets based on the provided configuration, scan type, and optional dataset name.

    Args:
        config (dict): The configuration dictionary containing cluster and dataset information.
        endpoint_url (str): The endpoint URL to send file information messages.
        scan_type (str): The type of scan to perform ('full' or 'incremental').
        file_extensions (list): List of allowed file extensions.
        dataset_name (str, optional): The name of the specific dataset to scan. Defaults to None.

    Returns:
        None
    """
    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M")
    current_time_dt = datetime.strptime(current_time, "%Y-%m-%d_%H-%M")
    for cluster in config['clusters']:
        for dataset in cluster['datasets']:
            if dataset_name and dataset['name'] != dataset_name:
                continue
            if not dataset['enabled']:
                log.error(f"Dataset {dataset['name']} is not enabled.")
                continue
            dataset_path = os.path.join(cluster['path'], dataset['name'])
            log.info(f"Processing dataset: {dataset_path}")

            # Set the start timestamp and failure flag
            dataset['start_time'] = current_time
            dataset['end_time'] = None
            dataset['status'] = 'IN_PROGRESS'
            save_config(config)

            try:
                if scan_type == 'full':
                    scan_full(dataset_path, endpoint_url, file_extensions, max_workers, dataset['name'])
                elif scan_type == 'incremental':
                    last_scan_timestamp = dataset.get('last_scan')
                    if not last_scan_timestamp:
                        log.error(f"No previous scan for dataset {dataset['name']}. Performing full scan.")
                        scan_full(dataset_path, endpoint_url, file_extensions, max_workers, dataset['name'])
                        dataset['last_scan'] = current_time
                        continue
                    last_scan_dt = datetime.strptime(last_scan_timestamp, "%Y-%m-%d_%H-%M")
                    scan_incremental(dataset_path, last_scan_dt, endpoint_url, file_extensions, max_workers,
                                     dataset['name'])
                    dataset['last_scan'] = current_time

                # Set end timestamp and success status
                dataset['end_time'] = datetime.now().strftime("%Y-%m-%d_%H-%M")
                dataset['status'] = 'SUCCESS'
            except Exception as e:
                log.error(f"Error processing dataset {dataset['name']}: {e}")
                dataset['status'] = 'FAILURE'
            finally:
                save_config(config)  # Ensure config is saved even if there is an exception


def main():
    parser = argparse.ArgumentParser(description='File Info Exporter')
    parser.add_argument('--scan', choices=['full', 'incremental'], help='Type of scan to execute')
    parser.add_argument('--dataset', help='Name of the dataset to scan')
    args = parser.parse_args()

    config = load_config()
    endpoint_url = config.get("endpoint_url", {}).get("url", "http://example.com/process")

    file_extensions = config['options']['file_extensions']

    global log  # Declare log as global to modify it within main
    log = Log(endpoint_url)
    log.info("Logging initialized successfully.")

    if args.scan:
        log.info(f"Starting a {args.scan} scan.")
        scan_datasets(config, endpoint_url, args.scan, file_extensions, args.dataset)
        log.info(f"{args.scan.capitalize()} scan completed.")
    else:
        log.info("no --scan argument found")
        exit()


if __name__ == "__main__":
    main()
