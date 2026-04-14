"""
Pipeline for fetching Oklahoma General Ledger data from the CKAN API and writing to S3.

On each run, the pipeline compares API resource metadata against records stored in DynamoDB.
New files are fetched and written to S3. Existing files are re-fetched and overwritten only
if their last_modified timestamp has changed. This handles both initial backfill and
incremental updates in a single pass.
"""

import logging
from datetime import (
    datetime,
    timezone,
)

from config.paths import (
    S3_BUCKET_NAME,
    S3_RAW_DIR,
)
from ingestion.fetch_data import (
    fetch_oklahoma_gl_csv_from_url,
    fetch_oklahoma_gl_resources,
)
from metadata.dynamodb import (
    get_metadata_item,
    put_metadata_item,
)
from s3.write_object import write_csv_object_to_s3

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s: %(message)s")


def run():
    # Retrieve oklahoma gl metadata from CKAN API
    resources = fetch_oklahoma_gl_resources()

    # Record count of files retrieved
    new_count = 0
    updated_count = 0
    skipped_count = 0

    for resource in resources:
        resource_id = resource["resource_id"]
        retrieved_at = datetime.now(timezone.utc).isoformat()

        existing = get_metadata_item(resource_id=resource_id)
        existing_item = existing.get("Item")

        if existing_item is None:
            # New file — fetch and write to S3
            file_name, file_data = fetch_oklahoma_gl_csv_from_url(resource=resource)
            write_csv_object_to_s3(
                bucket_name=S3_BUCKET_NAME, object_key=f"{S3_RAW_DIR}/{file_name}", file_data=file_data
            )
            put_metadata_item(resource=resource, retrieved_at=retrieved_at)
            new_count += 1
        else:
            # Existing file — check if last_modified has changed
            stored_last_modified = existing_item.get("last_modified", {}).get("S")
            if resource["last_modified"] != stored_last_modified:
                file_name, file_data = fetch_oklahoma_gl_csv_from_url(resource=resource)
                write_csv_object_to_s3(
                    bucket_name=S3_BUCKET_NAME, object_key=f"{S3_RAW_DIR}/{file_name}", file_data=file_data
                )
                put_metadata_item(resource=resource, retrieved_at=retrieved_at)
                updated_count += 1
            else:
                skipped_count += 1

    logging.info(
        "Pipeline complete — %d new, %d updated, %d unchanged (total: %d)",
        new_count,
        updated_count,
        skipped_count,
        len(resources),
    )


if __name__ == "__main__":
    run()
