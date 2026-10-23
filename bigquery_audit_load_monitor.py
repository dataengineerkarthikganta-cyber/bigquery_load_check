
"""
DAG: bigquery_audit_load_monitor

Purpose:
- Check the BigQuery ingestion audit table every hour.
- Detect the latest completed full load for a configured source table.
- Process each distinct load once.
- Skip processing if no new load is found.
- Save a checkpoint only after successful processing.

Assumption:
An audit row is written only after its corresponding full load
has completed successfully.
"""

import logging
from datetime import timedelta

import pendulum

from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.api_core.exceptions import NotFound
from google.cloud import bigquery


# ============================================================
# CONFIGURATION - UPDATE THESE VALUES
# ============================================================

PROJECT_ID = "your-gcp-project"

# Source table that gets deleted/reloaded from GCS
SOURCE_PROJECT = "your-gcp-project"
SOURCE_DATASET = "raw_dataset"
SOURCE_TABLE = "source_table"

# Audit table populated by your ingestion process
AUDIT_PROJECT = "your-gcp-project"
AUDIT_DATASET = "audit_dataset"
AUDIT_TABLE = "ingestion_audit"

# Checkpoint table used by this DAG
CONTROL_PROJECT = "your-gcp-project"
CONTROL_DATASET = "control_dataset"
CONTROL_TABLE = "dag_load_checkpoint"

# Airflow Google Cloud connection
GCP_CONN_ID = "google_cloud_default"

DAG_ID = "bigquery_audit_load_monitor"

# Audit column names - adjust to your real schema
AUDIT_PROJECT_COLUMN = "project_id"
AUDIT_DATASET_COLUMN = "dataset_name"
AUDIT_TABLE_COLUMN = "table_name"
AUDIT_FILENAME_COLUMN = "filename"
AUDIT_TIMESTAMP_COLUMN = "load_timestamp"

# Optional: set this to a column name if the audit table has a
# status column with a successful value such as "SUCCESS".
# Leave as None if audit rows are inserted only after a full
# load has completed successfully.
AUDIT_STATUS_COLUMN = None
AUDIT_SUCCESS_VALUE = "SUCCESS"

# Batch size to limit memory usage when reading the source.
# This example processes the first batch only, so the DAG will
# fail rather than checkpointing a partially processed load.
BATCH_SIZE = 10000


def get_bigquery_client():
    """Create a BigQuery client using the Airflow GCP connection."""
    hook = BigQueryHook(
        gcp_conn_id=GCP_CONN_ID,
        use_legacy_sql=False,
    )
    return hook.get_client(project_id=PROJECT_ID)


def validate_identifier(value, label):
    """Basic validation for configured SQL identifiers."""
    if not value or not all(
        part.replace("_", "").isalnum()
        for part in value.split(".")
    ):
        raise ValueError(f"Invalid {label}: {value}")


def source_table_id():
    return f"{SOURCE_PROJECT}.{SOURCE_DATASET}.{SOURCE_TABLE}"


def audit_table_id():
    return f"{AUDIT_PROJECT}.{AUDIT_DATASET}.{AUDIT_TABLE}"


def checkpoint_table_id():
    return f"{CONTROL_PROJECT}.{CONTROL_DATASET}.{CONTROL_TABLE}"


@dag(
    dag_id=DAG_ID,
    description=(
        "Detect new BigQuery full loads using the ingestion audit table"
    ),
    schedule="0 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["bigquery", "gcs", "audit", "full-load"],
)
def bigquery_audit_load_monitor():

    @task
    def validate_tables():
        """Verify that all required tables exist."""
        client = get_bigquery_client()

        for table_id in (
            source_table_id(),
            audit_table_id(),
            checkpoint_table_id(),
        ):
            try:
                table = client.get_table(table_id)
                logging.info(
                    "TABLE_EXISTS table=%s rows=%s",
                    table_id,
                    table.num_rows,
                )
            except NotFound as exc:
                raise ValueError(
                    f"Required BigQuery table does not exist: {table_id}"
                ) from exc

        return "All required tables exist"

    @task
    def detect_and_process_new_load():
        """
        Find the latest completed load, compare its identity with
        the checkpoint, and process it if it is new.

        Processing and checkpoint advancement are deliberately in
        one task to prevent downstream checkpointing after a
        failed processing task.
        """
        client = get_bigquery_client()

        # Validate configured identifiers.
        for identifier, label in [
            (SOURCE_PROJECT, "source project"),
            (SOURCE_DATASET, "source dataset"),
            (SOURCE_TABLE, "source table"),
            (AUDIT_PROJECT, "audit project"),
            (AUDIT_DATASET, "audit dataset"),
            (AUDIT_TABLE, "audit table"),
            (CONTROL_PROJECT, "control project"),
            (CONTROL_DATASET, "control dataset"),
            (CONTROL_TABLE, "checkpoint table"),
        ]:
            validate_identifier(identifier, label)

        source_id = source_table_id()
        audit_id = audit_table_id()
        checkpoint_id = checkpoint_table_id()

        # ----------------------------------------------------
        # STEP 1: Get the latest completed audit event
        # ----------------------------------------------------
        status_filter = ""
        if AUDIT_STATUS_COLUMN:
            validate_identifier(
                AUDIT_STATUS_COLUMN,
                "audit status column",
            )
            status_filter = (
                f"AND `{AUDIT_STATUS_COLUMN}` = @success_value"
            )

        audit_sql = f"""
            SELECT
                `{AUDIT_PROJECT_COLUMN}` AS project_id,
                `{AUDIT_DATASET_COLUMN}` AS dataset_name,
                `{AUDIT_TABLE_COLUMN}` AS table_name,
                `{AUDIT_FILENAME_COLUMN}` AS filename,
                `{AUDIT_TIMESTAMP_COLUMN}` AS load_timestamp
            FROM `{audit_id}`
            WHERE `{AUDIT_PROJECT_COLUMN}` = @source_project
              AND `{AUDIT_DATASET_COLUMN}` = @source_dataset
              AND `{AUDIT_TABLE_COLUMN}` = @source_table
              {status_filter}
            ORDER BY `{AUDIT_TIMESTAMP_COLUMN}` DESC,
                     `{AUDIT_FILENAME_COLUMN}` DESC
            LIMIT 1
        """

        audit_parameters = [
            bigquery.ScalarQueryParameter(
                "source_project", "STRING", SOURCE_PROJECT
            ),
            bigquery.ScalarQueryParameter(
                "source_dataset", "STRING", SOURCE_DATASET
            ),
            bigquery.ScalarQueryParameter(
                "source_table", "STRING", SOURCE_TABLE
            ),
        ]

        if AUDIT_STATUS_COLUMN:
            audit_parameters.append(
                bigquery.ScalarQueryParameter(
                    "success_value", "STRING", AUDIT_SUCCESS_VALUE
                )
            )

        audit_config = bigquery.QueryJobConfig(
            query_parameters=audit_parameters
        )

        audit_rows = list(
            client.query(
                audit_sql,
                job_config=audit_config,
            ).result()
        )

        if not audit_rows:
            logging.info(
                "NO_AUDIT_LOAD_FOUND source_table=%s",
                source_id,
            )
            return {
                "status": "NO_AUDIT_LOAD",
                "processed_count": 0,
            }

        latest = audit_rows[0]
        filename = latest["filename"]
        load_timestamp = latest["load_timestamp"]

        if not filename or not load_timestamp:
            raise ValueError(
                "Latest audit row has a null filename or load timestamp."
            )

        logging.info(
            "LATEST_AUDIT_LOAD filename=%s load_timestamp=%s",
            filename,
            load_timestamp,
        )

        # ----------------------------------------------------
        # STEP 2: Read the previous successful checkpoint
        # ----------------------------------------------------
        checkpoint_sql = f"""
            SELECT
                last_processed_filename,
                last_processed_load_timestamp
            FROM `{checkpoint_id}`
            WHERE dag_id = @dag_id
              AND source_project = @source_project
              AND source_dataset = @source_dataset
              AND source_table = @source_table
            LIMIT 1
        """

        checkpoint_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "dag_id", "STRING", DAG_ID
                ),
                bigquery.ScalarQueryParameter(
                    "source_project", "STRING", SOURCE_PROJECT
                ),
                bigquery.ScalarQueryParameter(
                    "source_dataset", "STRING", SOURCE_DATASET
                ),
                bigquery.ScalarQueryParameter(
                    "source_table", "STRING", SOURCE_TABLE
                ),
            ]
        )

        checkpoint_rows = list(
            client.query(
                checkpoint_sql,
                job_config=checkpoint_config,
            ).result()
        )

        if checkpoint_rows:
            checkpoint = checkpoint_rows[0]
            already_processed = (
                checkpoint["last_processed_filename"] == filename
                and checkpoint["last_processed_load_timestamp"]
                == load_timestamp
            )
        else:
            already_processed = False

        # ----------------------------------------------------
        # STEP 3: Skip if this load was already processed
        # ----------------------------------------------------
        if already_processed:
            logging.info(
                "NO_NEW_LOAD latest_filename=%s "
                "latest_load_timestamp=%s",
                filename,
                load_timestamp,
            )
            return {
                "status": "SKIPPED_ALREADY_PROCESSED",
                "filename": filename,
                "processed_count": 0,
            }

        logging.info(
            "NEW_LOAD_DETECTED filename=%s load_timestamp=%s",
            filename,
            load_timestamp,
        )

        # ----------------------------------------------------
        # STEP 4: Read and process the current source table
        # ----------------------------------------------------
        #
        # Replace the sample processing block below with your
        # real business logic or destination write.
        #
        # Important: this sample reads at most BATCH_SIZE rows.
        # If more rows exist, it fails and does not save the
        # checkpoint, preventing a partial load from being
        # incorrectly marked as complete.
        #
        source_sql = f"""
            SELECT *
            FROM `{source_id}`
            LIMIT @batch_size
        """

        source_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "batch_size", "INT64", BATCH_SIZE
                )
            ]
        )

        source_job = client.query(
            source_sql,
            job_config=source_config,
        )

        rows = list(source_job.result())
        total_rows = source_job.num_dml_affected_rows

        # num_dml_affected_rows is not populated for SELECT jobs.
        # Get the exact source count separately.
        count_sql = f"SELECT COUNT(*) AS row_count FROM `{source_id}`"
        count_result = list(client.query(count_sql).result())
        total_rows = count_result[0]["row_count"]

        if total_rows > BATCH_SIZE:
            raise RuntimeError(
                f"Source contains {total_rows} rows, exceeding the "
                f"sample batch limit of {BATCH_SIZE}. Implement complete "
                "batch processing before enabling checkpoint advancement."
            )

        logging.info(
            "SOURCE_ROWS_READ count=%d filename=%s",
            len(rows),
            filename,
        )

        # Example processing: log completion and row count.
        # Add your actual transformations and destination writes here.
        # For a real pipeline, make writes idempotent.
        for row in rows:
            logging.debug("SOURCE_RECORD %s", dict(row.items()))

        logging.info(
            "PROCESSING_COMPLETED filename=%s row_count=%d",
            filename,
            len(rows),
        )

        # ----------------------------------------------------
        # STEP 5: Save checkpoint only after processing succeeds
        # ----------------------------------------------------
        #
        # A MERGE keeps one checkpoint row per source table and DAG.
        #
        checkpoint_merge_sql = f"""
            MERGE `{checkpoint_id}` AS target
            USING (
                SELECT
                    @dag_id AS dag_id,
                    @source_project AS source_project,
                    @source_dataset AS source_dataset,
                    @source_table AS source_table,
                    @filename AS last_processed_filename,
                    @load_timestamp AS last_processed_load_timestamp+
					
            ) AS incoming
            ON target.dag_id = incoming.dag_id
               AND target.source_project = incoming.source_project
               AND target.source_dataset = incoming.source_dataset
               AND target.source_table = incoming.source_table

            WHEN MATCHED THEN
                UPDATE SET
                    last_processed_filename =
                        incoming.last_processed_filename,
                    last_processed_load_timestamp =
                        incoming.last_processed_load_timestamp

            WHEN NOT MATCHED THEN
                INSERT (
                    dag_id,
                    source_project,
                    source_dataset,
                    source_table,
                    last_processed_filename,
                    last_processed_load_timestamp
                )
                VALUES (
                    incoming.dag_id,
                    incoming.source_project,
                    incoming.source_dataset,
                    incoming.source_table,
                    incoming.last_processed_filename,
                    incoming.last_processed_load_timestamp
                )
        """

        merge_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "dag_id", "STRING", DAG_ID
                ),
                bigquery.ScalarQueryParameter(
                    "source_project", "STRING", SOURCE_PROJECT
                ),
                bigquery.ScalarQueryParameter(
                    "source_dataset", "STRING", SOURCE_DATASET
                ),
                bigquery.ScalarQueryParameter(
                    "source_table", "STRING", SOURCE_TABLE
                ),
                bigquery.ScalarQueryParameter(
                    "filename", "STRING", filename
                ),
                bigquery.ScalarQueryParameter(
                    "load_timestamp", "TIMESTAMP", load_timestamp
                ),
            ]
        )

        client.query(
            checkpoint_merge_sql,
            job_config=merge_config,
        ).result()

        logging.info(
            "CHECKPOINT_SAVED filename=%s load_timestamp=%s "
            "processed_rows=%d",
            filename,
            load_timestamp,
            len(rows),
        )

        return {
            "status": "PROCESSED",
            "filename": filename,
            "processed_count": len(rows),
        }

    validation = validate_tables()
    processing = detect_and_process_new_load()

    # Ensure validation completes before processing starts.
    validation >> processing


bigquery_audit_load_monitor()
