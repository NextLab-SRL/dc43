# AWS Fargate deployment

This Terraform stack deploys the dc43 HTTP service backends on Amazon ECS with
Fargate. It provisions:

- An ECS cluster and task definition running the `dc43-service-backends-http`
  container
- An Amazon EFS file system mounted into the task for persistent contracts when
  `contract_store_mode = "filesystem"`
- Environment variables or Secrets Manager references for the SQL contract
  store when `contract_store_mode = "sql"`
- CloudWatch logging, IAM roles, and execution policies
- An Application Load Balancer fronting the service with TLS termination

## Usage

1. Build the container image and push it to Amazon ECR.
2. Populate a `terraform.tfvars` file with the required variables (see below).
3. Apply the configuration from the repository root:

   ```bash
   terraform -chdir=deploy/terraform/aws-service-backend init
   terraform -chdir=deploy/terraform/aws-service-backend apply
   ```

The deployment expects existing networking primitives (subnets and security
groups) so it can plug into your VPC topology.

## Required variables

| Name | Description |
| ---- | ----------- |
| `aws_region` | AWS region to deploy into (e.g. `eu-west-1`). |
| `cluster_name` | Name of the ECS cluster. |
| `ecr_image_uri` | Fully qualified image URI (including tag) in ECR. |
| `backend_token` | Optional bearer token enforced by the service. |
| `contract_filesystem` | Name used for the EFS file system (filesystem mode). |
| `private_subnet_ids` | Private subnet IDs where the Fargate tasks run. |
| `load_balancer_subnet_ids` | Subnets for the public Application Load Balancer. |
| `service_security_group_id` | Security group applied to the ECS service ENI. |
| `load_balancer_security_group_id` | Security group attached to the ALB. |
| `certificate_arn` | ACM certificate ARN for HTTPS. |
| `vpc_id` | VPC that hosts the deployment. |

Optional variables let you tune CPU/memory, desired task count, and health check
thresholds—check `variables.tf` for the full list.

### Switching to the SQL contract store

Set `contract_store_mode = "sql"` to bypass the EFS share and inject the
relational backend configuration. In this mode, provide either a DSN directly or
reference a secret:

| Variable | Description |
| -------- | ----------- |
| `contract_store_dsn` | SQLAlchemy DSN with credentials (for example `postgresql+psycopg://user:pass@rds.internal/dc43`). |
| `contract_store_dsn_secret_arn` | Secrets Manager or SSM Parameter ARN resolved by ECS at runtime (preferred for production). |
| `contract_store_table` | Optional table name (defaults to `contracts`). |
| `contract_store_schema` | Optional schema/namespace containing the table. |

## Outputs

- `load_balancer_dns_name` – Public DNS of the Application Load Balancer.
- `service_name` – Name of the ECS service.
- `efs_id` – ID of the EFS file system that stores contracts (null when
  `contract_store_mode = "sql"`).

Upload approved contracts or seed drafts by writing to the EFS share exposed to
the tasks.
