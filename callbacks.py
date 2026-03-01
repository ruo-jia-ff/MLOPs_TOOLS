from azure_blob_user import create_blob_storage_client_via_key, AzureBlobUser
from postgres_client import PostgresClient

import os
import uuid
import torch

from datetime import datetime
from dotenv import load_dotenv

# Postgres and checkpoint from env values
load_dotenv(".env.postgres")
load_dotenv(".env.checkpoints")

#########################################################################
##################  CALL BACK FOR BLOB_STORAGE UPDATE ###################
#########################################################################

# ---------------------- AZURE BLOB UPLOAD CALL BACK---------------------

###### MAKE SURE THE EXPIRY DOESN"T BECOME A PROBLEM!!!!!!
blob_storage_client, expiry = create_blob_storage_client_via_key() 
blob_user = AzureBlobUser(blob_storage_client, expiry)

class AzureCheckpointCallback:
    def __init__(self, 
                 project_name,
                 blob_user = blob_user,
                 run_id=None):
        
        if not project_name:
            raise ValueError("project_name must be provided.")

        checkpoint_dir = os.getenv("CHECKPOINT_DIR")
        blob_container_name = os.getenv("BLOB_CONTAINER")

        if not checkpoint_dir:
            raise ValueError("CHECKPOINT_DIR not set in .env.checkpoints")

        if not blob_container_name:
            raise ValueError("BLOB_CONTAINER not set in .env.checkpoints")

        self.blob_user = blob_user
        self.blob_container_name = blob_container_name
        self.checkpoint_dir = checkpoint_dir
        self.project_name = project_name.replace(" ", "_")
        self.run_id = run_id or str(uuid.uuid4())[:8]
        self.blob_user.establish_blob_container(self.blob_container_name)

        os.makedirs(self.checkpoint_dir, exist_ok=True)

    def end_of_epoch_activity(self, trainer):

        checkpoint_file = os.path.join(
            self.checkpoint_dir,
            f"model_epoch_{trainer.epoch}.pth"
        )

        torch.save({
            "epoch": trainer.epoch,
            "model_state_dict": trainer.model.state_dict(),
            "optimizer_state_dict": trainer.optimizer.state_dict(),
            "val_loss": trainer.val_loss,
            "val_acc": trainer.val_acc,
        }, checkpoint_file)

        # Construct blob name
        timestamp = datetime.utcnow().strftime("%y%m%d%H%M%S")
        blob_name = (
            f"{self.project_name}-"
            f"{self.run_id}-"
            f"e{trainer.epoch:03d}-"
            f"{timestamp}.pth"
        )

        upload_success = True
        upload_error = None

        try:

            if not isinstance(self.blob_user, AzureBlobUser):  # Ensure it's the right type
                raise TypeError(f"Expected AzureBlobUser, got {type(self.blob_user)}.")

            # Upload to Azure
            self.blob_user.upload_file_to_blob_container(
                checkpoint_file,
                self.blob_container_name,
                blob_name
            )

            if os.path.exists(checkpoint_file):
                os.remove(checkpoint_file)

        except Exception as e:
            upload_success = False
            upload_error = str(e)
            print(f"[WARNING] Checkpoint upload failed: {e}")

        # Expose results to trainer (used by Postgres logger)
        trainer.checkpoint_path = f"{self.blob_container_name}/{blob_name}"
        trainer.checkpoint_upload_success = upload_success
        trainer.checkpoint_upload_error = upload_error
        trainer.run_id = self.run_id

#########################################################################
##################  CALL BACK FOR POSTGRES UPDATE #######################
#########################################################################

# ---------------------- Postgres Logging Callback ----------------------
class PostgresLoggingCallback:
    def __init__(self, 
                 metadata=None, 
                 run_id=None, 
                 db_name = None,
                 table_name=None,
                 custom_fields=None):
        """
        metadata : optional dict with static run metadata
        run_id : optional unique id for this training run
        db_name : optional override of env database
        table_name : optional override of env table
        """
        self.pg_client = PostgresClient(
                            user=os.getenv("ML_WRITER_USER"),
                            password=os.getenv("ML_WRITER_PW"),
                            host=os.getenv("HOST"),
                            port=int(os.getenv("PORT", 5432)),
                            db_name=os.getenv("TRAIN_DATABASE"),
                            table_name=os.getenv("TRAIN_LOG_TABLE"),
                            initialize_on_construction = True
                            )

        # Override database if provided
        if db_name:
            self.pg_client.switch_db(db_name)
            print(f"[INFO] Switched to database: {db_name}")

        # Override table if provided
        if not self.pg_client.table_name:
            self.pg_client.set_table(table_name)
            print(f"[INFO] Loggin will use table: {table_name}")

        # Check we have a table to write to
        if not self.pg_client.table_name:
            raise ValueError(
                "No table set. Set TRAIN_LOG_TABLE in .env.postgres or pass table_name."
            )

        # Unique run ID and metadata
        self.run_id = run_id or str(uuid.uuid4())
        self.metadata = metadata or {}
        self.custom_fields = custom_fields or {}

        # Load the keys to extract from the .env
        self.keys_we_want = os.getenv("TRAIN_LOG_KEYS", "").split(',')
        self.keys_we_want = [key.strip() for key in self.keys_we_want]  # Clean extra spaces

    def _construct_row_to_upload(self, trainer, row_to_upload = None):
        """
        Constructs the row to be uploaded to the database.
        If row_to_upload is None, it dynamically builds it from the trainer's attributes.
        """
        if not isinstance(row_to_upload, dict):
            row_to_upload = {}

            # Include metadata
            row_to_upload.update(self.metadata)

            # Dynamically extract relevant attributes from the trainer
            keys_we_want = set(['project_name', 'run_id', 'epoch', 'val_loss', 'val_acc', 
                                'learning_rate', 'checkpoint_path', 'checkpoint_upload_success',
                                'checkpoint_upload_error', 'average_train_loss', 'average_train_accuracy'])

            for key, value in trainer.__dict__.items():
                if key in keys_we_want:
                    row_to_upload[key] = value

            print("Using DATA from Trainer...")

        return row_to_upload

    def end_of_epoch_activity(self, trainer, row_to_upload = None):

        # Check if there's anyting to upload
        if not (row_to_upload or trainer):
            print("There is NO DATA to upload...")
            return None
        
        row_to_upload = self._construct_row_to_upload(trainer, row_to_upload)

        # Upload the data to postgres
        try:
            with self.pg_client:
                # Wrap single dict in a list for bulk insert
                self.pg_client.insert_values_into_table(
                    table=self.pg_client.table_name,
                    data_as_dicts=row_to_upload
                )
                
        except Exception as e:
            print(f"[WARNING] Postgres logging failed: {e}")