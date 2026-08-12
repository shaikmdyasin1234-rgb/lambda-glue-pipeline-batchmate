# AWS S3 → Lambda → Glue data pipeline

This repository deploys an AWS data pipeline with **AWS CloudFormation** and a **GitHub Actions OIDC** deployment role.

## Architecture

```text
GitHub Actions
     │
     │ OIDC (no long-lived AWS keys)
     ▼
AWS IAM Deploy Role
     │
     ▼
CloudFormation
     │
     ├── S3 bucket
     │     ├── landing/
     │     └── output/
     │
     ├── Lambda
     │     └── validates CSV columns
     │
     └── Glue Job
           └── reads CSV → writes Parquet partitioned by city/state/country

S3 landing/*.csv
      │
      ▼
Lambda
      │ valid schema
      ▼
Glue Job
      │
      ▼
s3://<bucket>/output/city=<city>/state=<state>/country=<country>/
```

> The Glue job partitions by **city + state + country**. Spark may create multiple `part-*.parquet` files inside each partition; "three files" is interpreted here as three partition dimensions rather than exactly three physical files.

## Expected input

CSV files must contain these columns:

```text
city,state,country
```

Additional columns are allowed.

Example:

```csv
id,name,city,state,country
1,Alice,Hyderabad,Telangana,India
2,Bob,Vijayawada,Andhra Pradesh,India
```

Upload input files under:

```text
landing/
```

Only `.csv` objects under `landing/` trigger the Lambda.

## Repository layout

```text
.
├── .github/
│   └── workflows/
│       └── deploy.yml
├── cfn/
│   └── pipeline.yaml
├── glue/
│   └── job.py
├── lambda/
│   └── handler.py
├── sample-data/
│   └── sample.csv
├── scripts/
│   └── package.sh
├── .gitignore
└── README.md
```

## Prerequisites

1. An AWS account.
2. A GitHub repository.
3. GitHub Actions enabled.
4. Permission to create the resources in `cfn/pipeline.yaml`.
5. AWS CLI available locally if you want to bootstrap the GitHub OIDC role manually.

## OIDC security model

The repository uses GitHub Actions OIDC, so **no AWS access key or secret key is stored in GitHub**.

There are two CloudFormation stacks:

1. `bootstrap-github-oidc.yaml` — deployed once by an AWS administrator to create the GitHub OIDC provider and deployment role.
2. `pipeline.yaml` — deployed automatically by GitHub Actions.

The role trust policy is restricted to:

```text
repo:<GitHub owner>/<GitHub repository>:ref:refs/heads/<branch>
```

The GitHub workflow uses:

```yaml
permissions:
  id-token: write
  contents: read
```

### One-time OIDC bootstrap

From an administrator-authenticated AWS CLI session:

```bash
aws cloudformation deploy   --template-file cfn/bootstrap-github-oidc.yaml   --stack-name github-actions-oidc-bootstrap   --parameter-overrides     GitHubOrg=YOUR_GITHUB_ORG     GitHubRepo=YOUR_GITHUB_REPO     GitHubBranch=main   --capabilities CAPABILITY_NAMED_IAM
```

Get the role ARN:

```bash
aws cloudformation describe-stacks   --stack-name github-actions-oidc-bootstrap   --query 'Stacks[0].Outputs[?OutputKey==`GitHubActionsRoleArn`].OutputValue'   --output text
```

Add that value to the GitHub repository variable:

```text
AWS_DEPLOY_ROLE_ARN
```

If your AWS account already has the GitHub OIDC provider, do not create a duplicate provider. Reuse the existing provider and adapt the bootstrap template accordingly.

> The bootstrap example uses `AdministratorAccess` so the demonstration works without designing a deployment policy for every AWS resource. For production, replace it with a least-privilege policy or a CloudFormation execution-role design.

## Deploying

The workflow uses:

```yaml
AWS_REGION: ap-south-1
STACK_NAME: s3-lambda-glue-pipeline
```

You can change these values in `.github/workflows/deploy.yml`.

After the one-time bootstrap, push to `main`. The workflow will:

1. Check out the repository.
2. Configure AWS credentials using GitHub OIDC.
3. Package the Lambda and Glue source into the CloudFormation stack.
4. Deploy the CloudFormation stack.
5. Wait for completion.
6. Print stack outputs.

## Testing

After deployment, get the bucket name from the CloudFormation output:

```bash
aws cloudformation describe-stacks \
  --stack-name s3-lambda-glue-pipeline \
  --query 'Stacks[0].Outputs'
```

Upload:

```bash
aws s3 cp sample-data/sample.csv s3://YOUR_BUCKET/landing/sample.csv
```

The sequence is:

```text
S3 upload
  ↓
Lambda S3 event
  ↓
CSV header validation
  ↓
Glue StartJobRun
  ↓
Glue reads CSV
  ↓
Parquet output partitioned by city/state/country
```

## Failure behavior

### Missing required columns

Lambda logs the missing columns and does not start Glue.

Example:

```text
Missing required columns: ['country']
```

### Empty file

Lambda rejects the file.

### Wrong extension

Only `.csv` objects are accepted by the S3 notification filter.

### Glue failure

The Glue job failure is visible in AWS Glue and CloudWatch Logs.

## Output example

For:

```text
city=Hyderabad
state=Telangana
country=India
```

the output looks like:

```text
s3://YOUR_BUCKET/output/
└── city=Hyderabad/
    └── state=Telangana/
        └── country=India/
            └── part-....parquet
```

## Production improvements

For production, consider adding:

- AWS Lake Formation
- Glue Data Catalog table
- Athena
- S3 versioning
- S3 lifecycle policies
- KMS encryption with customer-managed keys
- Dead-letter queue / EventBridge retry strategy
- Lambda Powertools
- Glue job bookmarks
- schema registry / data contract
- duplicate-event/idempotency handling
- CloudWatch alarms
- separate dev/stage/prod stacks
- a pre-created OIDC deployment role with a least-privilege deployment policy
