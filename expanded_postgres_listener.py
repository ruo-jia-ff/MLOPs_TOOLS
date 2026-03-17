from postgres_client import PostgresClient
from blob_and_docker_tools import acquire_blob_given_blobpath, run_downloaded_blob_via_docker
import os

import json
import traceback
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from collections import defaultdict
import select

import platform
import psutil, GPUtil
import socket

load_dotenv(".env.postgres")
load_dotenv(".env.postgres.mlops")
load_dotenv(".env.checkpoints")

mlops_db = os.environ.get("MLOPS_DATABASE")
log_table = os.environ.get("MLOPS_LOG_TABLE")
print(log_table)
job_table = os.environ.get("MLOPS_JOB_TABLE")
worker_table = os.environ.get("MLOPS_WORKER_TABLE")
worker = socket.gethostname()

def get_system_info():
    freq = psutil.cpu_freq()
    info = {
        "worker_name": socket.gethostname(),
        "processor": platform.processor(),
        "cpu_cores": psutil.cpu_count(logical=False),
        "cpu_threads": psutil.cpu_count(logical=True),
        "cpu_mhz_max": round(freq.max, 2),
        "ram_gb": round(psutil.virtual_memory().total / (1024 ** 3), 2),
        "gpu": None,
        "gpu_ram_gb": None,
        "worker_available": True
    }

    try:
        gpus = GPUtil.getGPUs()
        if gpus:
            info["gpu"] = gpus[0].name
            info["gpu_ram_gb"] = round(gpus[0].memoryTotal / 1024, 2)
    except ImportError:
        pass

    return info

class PostgresListener(PostgresClient):

    def __init__(self, 
                 user, 
                 password, 
                 host, 
                 port, 
                 db_name,
                 table_name = None,
                 autocommit = True,
                 process_dict = {"check: ml_tasks": lambda x: x},
                 initialize_on_construction = True):

        super().__init__(user, 
                         password, 
                         host, 
                         port, 
                         db_name, 
                         table_name, 
                         autocommit, 
                         initialize_on_construction)
        
        self.process_dict = defaultdict(lambda: None, process_dict)
        self.job_id = None
        self.blob_path = None
        self.docker_image_name = None
        self.worker_name = worker
        self._make_sure_listener_in_worker_table()

    def _make_sure_listener_in_worker_table(self):

        listener_info = get_system_info()

        # Is the worker in system?
        this_worker = self.fetch_n_rows_from_table(
                           table=worker_table,
                           return_results_formatted=True,
                           show_id=True,
                           where_conditions={"worker_name": self.worker_name})

        if not this_worker: # If not create it
            self.insert_values_into_table(data_as_dicts=listener_info, 
                                          table=worker_table,
                                          custom_message="Inserted worker info into worker table!")

        else: # If so, let system know it is now available for work 
            self.update_row_values(where_conditions={"worker_name": self.worker_name},
                                   set_values={"worker_available": True},
                                   table = worker_table)

    def _format_error_for_pg(self, e: Exception) -> str:
        return json.dumps({
            "type": type(e).__name__,
            "message": str(e),
            "traceback": traceback.format_exc()
        })

    def _update_job_status(self, filter_condition, new_values, table = None):

        table = table or self.table_name
        self.update_row_values(
            table=table,
            set_values=new_values,
            where_conditions=filter_condition,
            return_updated_rows=True
        )

    def fetch_next_queued_job(self, 
                            table=job_table, 
                            status_col='job_status', 
                            queued_value='Queued'):
        
        """Atomically fetch and claim the next queued job."""
        query = f"""
            UPDATE {table}
            SET {status_col} = 'Processing',
                worker_name = %s,
                started_at = %s
            WHERE id = (
                SELECT id FROM {table}
                WHERE {status_col} = %s
                ORDER BY id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING *;
        """
        
        with self.conn.cursor() as cursor:
            cursor.execute(query, [self.worker_name, datetime.now(timezone.utc), queued_value])
            result = cursor.fetchone()
            self.conn.commit()

        print(result)
        if not result:
            return False

        column_names = [desc[0] for desc in cursor.description]
        result = {k: v for k, v in zip(column_names, list(result))}

        self.job_id = result.get('id')
        self.blob_path = result.get('docker_image_blob_path')
        self.docker_image_name = result.get('docker_image_name')
        print(f"Running: {self.job_id}, its docker image is {self.docker_image_name}")

        return True

    def release_jobs_assigned_to_worker(self, 
                                        job_table=job_table,
                                        worker=None, # Default to self.worker_name
                                        timeout_minutes=0):
        
        worker = worker or self.worker_name

        query = f"""
            UPDATE {job_table}
            SET job_status = 'Queued',
                worker_name = NULL,
                started_at = NULL
            WHERE job_status = 'Processing'
            AND worker_name = %s
            AND started_at < NOW() - INTERVAL '{timeout_minutes} minutes'
            RETURNING id;
        """
        
        with self.conn.cursor() as cursor:
            cursor.execute(query, [worker])
            released = cursor.fetchall()
            self.conn.commit()

        if released:
            print(f"Worker: [{worker}] released from jobs: {[r[0] for r in released]}...")
            print("Sending notification to workers to get these jobs..")
            self.send_notification()

    def update_mlops_log(self, 
                         current_status,
                         error_message = None,
                         notes = None,
                         num_attempts = 3):

        # Create a separate client to interact with the mlop_log_table

        job_id = self.job_id or -1
        docker_image_name = self.docker_image_name or "Not Applicable" 

        try:
            with PostgresClient(
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                db_name=self.db_name,
                table_name=log_table,
                initialize_on_construction = True) as ml_logger:           

                    log_values = {
                        "job_id": job_id,
                        "docker_image_name": docker_image_name,
                        "current_status": current_status,
                        "worker_name": self.worker_name,
                        "notes": notes,
                        "error_message": error_message,
                    }

                    for attempt in range(num_attempts):
                        try:
                            ml_logger.insert_values_into_table(data_as_dicts=log_values,
                                                               custom_message=f"Logs added to mlops_log_table. Current Status: {current_status}")
                            break

                        except Exception as log_err:
                            print(f"Log attempt {attempt+1}/{num_attempts} failed: {log_err}")
                            time.sleep(10)

                            if (attempt + 1) == num_attempts:
                                print(f"All {num_attempts} log attempts failed. Giving up.")

        except Exception as e:
            print(f"Falling back to local log file: {e}")
            self._write_local_log(log_values, error=str(e))

    def _write_local_log(self, log_values: dict, error: str = None):
        """Write log entry to a local .log file as fallback."""
        log_path = f"mlops_fallback_{self.worker_name}.log"
        timestamp = datetime.now(timezone.utc).isoformat()

        lines = [
            f"\n--- {timestamp} ---",
            f"DB write failed: {error}",
        ] + [f"{k}: {v}" for k, v in log_values.items()]

        with open(log_path, "a") as f:
            f.write("\n".join(lines) + "\n")

        print(f"Log written to {log_path}")

    def listen_for_notifications(self, channel, timeout=10, release_jobs = True, verbose=False):
        """Listen for notifications on the specified channel."""
        try:
            if not self.conn:
                raise Exception("Connection not established. Please connect first.")
            
            # Release any stuck jobs from a previous runs
            if release_jobs:
                self.release_jobs_assigned_to_worker()

            with self.conn.cursor() as cur:
                cur.execute(f"LISTEN {channel};")
                print(f"Listening on channel: {channel}")

                while True:
                    try:
                        if not self._connection_is_alive():
                            print("Connection lost. Attempting to reconnect...")
                            if not self._reconnect():
                                raise RuntimeError("Could not restore database connection. Shutting down listener.")
                            with self.conn.cursor() as cur:
                                cur.execute(f"LISTEN {channel};")
                            print(f"Re-listening on channel: {channel}")

                        if select.select([self.conn], [], [], timeout) == ([], [], []):
#                            if verbose:
#                                print(f"No notifications in the last {timeout} seconds.")
                            continue

                        self.conn.poll()

                        while self.conn.notifies:
                            notify = self.conn.notifies.pop()
                            print(f"Received notification on channel: '{notify.channel}': {notify.payload}")

                            # Atomically claim a job before processing
                            if not self.fetch_next_queued_job():
                                print("No queued jobs found, skipping...")
                                continue

                            try:
                                self.process_payload()
                            except Exception as payload_err:
                                if verbose:
                                    print(f"Payload error: {payload_err}\n{traceback.format_exc()}")

                                err_msg = self._format_error_for_pg(payload_err)                               
                                self.update_mlops_log(current_status="payload processing error", 
                                                      error_message=err_msg)

                    # propagate reconnect failure up to outer except, clean shut down 
                    except RuntimeError:
                        raise  

                    except Exception as loop_err:
                        if verbose:
                            print(f"Looping error: {loop_err}\n{traceback.format_exc()}")

                        err_msg = self._format_error_for_pg(loop_err)                               
                        self.update_mlops_log(current_status="looping error", 
                                              error_message=err_msg)
                        
                        time.sleep(5)

        except Exception as listen_err:            
            if verbose:
                print(f"Listener initialization error: {listen_err}\n{traceback.format_exc()}")

            err_msg = self._format_error_for_pg(listen_err)                               
            self.update_mlops_log(current_status="listening error", 
                                  error_message=err_msg)

    def process_payload(self):
        """Download payload and execute job."""

        try:
            print(f"Trying to download docker image: {self.blob_path}")
            tar_file_path = acquire_blob_given_blobpath(self.blob_path)

            print("Starting job...")
            self.update_mlops_log(current_status="Started")

            run_downloaded_blob_via_docker(tar_file_path)

            print("Finished job...")
            filter_condition = {"docker_image_blob_path": self.blob_path}
            self.update_mlops_log(current_status="Finished")
            finished_job_values = {"job_status": "Completed", "finished_at": datetime.now(timezone.utc)}
            self._update_job_status(filter_condition=filter_condition, 
                                    new_values = finished_job_values)
            
            print("Switching back to listening...")

        except Exception as e:
            print(f"Job execution failed: {e}\n{traceback.format_exc()}")
            error_message = self._format_error_for_pg(e)
           
            self.update_mlops_log(current_status="Error running docker image",
                                  error_message=error_message)

            # Reset job status
            try:
                finished_job_values = {"job_status": "Queued", "started_at": None}
                self._update_job_status(filter_condition=filter_condition, 
                                        new_values = finished_job_values)
                
            except Exception:
                pass
