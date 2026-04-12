# Oklahoma General Ledger Pipeline

A data pipeline on AWS that ingests, transforms, and serves Oklahoma state expenditure data for analysis.

## Architecture (Planned Workflow)

```
CKAN API → ECS Fargate (ingest) → S3 raw → Glue (transform) → S3 silver → Athena
                  ↓
              DynamoDB (CDC metadata)
```

## Infrastructure

All resources defined as CloudFormation — deployed via `make` targets in `infra/`.

| Stack | Description |
|---|---|
| `okgl-iam` | IAM roles and policies for Glue, ECR |
| `okgl-metadata` | DynamoDB table for CDC metadata |
| `okgl-ecr` | ECR repository for the ingestion container image |
| `okgl-glue` | Glue ETL job definition |

## Project Structure

```
src/
  ingestion/    # CKAN API fetch logic
  s3/           # S3 read/write helpers
  metadata/     # DynamoDB CDC metadata
  config/       # Constants and S3 paths
  glue_jobs/    # PySpark transformation (runs on AWS Glue)
infra/
  cloudformation/   # CloudFormation templates
  Makefile          # Deploy and container commands
run_pipeline.py     # Container entry point
```

## Data Source

- **Dataset**: Oklahoma General Ledger
- **Provider**: Oklahoma Office of Management and Enterprise Services (OMES)
- **License**: [CC-BY](https://creativecommons.org/licenses/by/4.0/)
- **URL**: [data.ok.gov](https://data.ok.gov/dataset/general-ledger)
