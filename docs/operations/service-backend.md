# Operations: publish shared service backends

Use this guide when you run the dc43 governance services for your teams. It walks through packaging the HTTP backends, validating
configuration locally, and deploying them to Azure Container Apps or AWS ECS Fargate with the Terraform modules shipped in this
repository.

## Prerequisites

- Docker CLI for building images.
- Terraform 1.5+ for infrastructure deployments.
- Access to a container registry (Azure Container Registry or Amazon ECR).
- Python 3.11 to edit configuration and run local smoke tests.

The governance services read their configuration from TOML files. Start from the templates under [`docs/templates/`](../templates/)
and adapt [`service-backends-configuration.md`](../service-backends-configuration.md) to your storage toolchain (filesystem, Delta,
Collibra, ...).

## 1. Build and test the HTTP container

The FastAPI surface lives under `deploy/http-backend`. Build the container with the HTTP extra so runtime dependencies are baked
in. You can either invoke Docker directly or use the helper script that wraps the build and optional push:

```bash
# Build only
python scripts/package_http_backend.py --image dc43-service-backends-http:local

# Build and push to a registry in one step
python scripts/package_http_backend.py \
  --image myregistry.azurecr.io/dc43/governance:latest \
  --push
```

When using the raw Docker CLI the equivalent command is:

```bash
docker build -t dc43-service-backends-http -f deploy/http-backend/Dockerfile .
```

Smoke test the image before publishing it. Mount a contract directory and inject the shared token the clients will use:

```bash
docker run --rm \
  -p 8001:8001 \
  -e DC43_BACKEND_TOKEN="super-secret" \
  -v $(pwd)/contracts:/contracts \
  dc43-service-backends-http
```

Visit `http://localhost:8001/docs` to confirm the FastAPI app boots. Stop the container once probes return `200 OK`.

Push the image to your registry once the smoke test passes (replace the example tags with your own registry URI). The helper
script automatically pushes when `--push` is provided. When pushing manually run:

```bash
docker tag dc43-service-backends-http myregistry.azurecr.io/dc43/governance:latest
docker push myregistry.azurecr.io/dc43/governance:latest
```

## 2. (Optional) Publish the contracts helper UI

Teams who prefer a ready-to-use portal for browsing datasets and generating
integration stubs can run the contracts app as a companion service. Build the
image with the same helper script, selecting the `contracts-app` target:

```bash
# Build the contracts app locally
python scripts/package_http_backend.py --target contracts-app --image dc43-contracts-app:local

# Build and push to a registry
python scripts/package_http_backend.py \
  --target contracts-app \
  --image myregistry.azurecr.io/dc43/contracts-app:latest \
  --push
```

Run the container next to the HTTP backend and point it at the shared service:

```bash
docker run --rm \
  -p 8000:8000 \
  -e DC43_CONTRACTS_APP_BACKEND_URL="https://governance.example.com" \
  -e DC43_BACKEND_TOKEN="super-secret" \
  myregistry.azurecr.io/dc43/contracts-app:latest
```

The [contracts app README](../../deploy/contracts-app/README.md) covers
additional environment variables for embedded mode and local storage mounts.

## 3. Provision shared infrastructure

dc43 ships Terraform recipes you can adapt to your naming standards. Provide real values through `terraform.tfvars` instead of
hard-coding secrets in `main.tf`.

### 2.1 Azure Container Apps

The module in [`deploy/terraform/azure-service-backend/`](../../deploy/terraform/azure-service-backend/) provisions the resource
group dependencies, Log Analytics workspace, Container Apps environment, and the container app itself.

Sample `terraform.tfvars`:

```hcl
subscription_id     = "00000000-0000-0000-0000-000000000000"
resource_group_name = "rg-dc43-governance"
location            = "westeurope"
container_registry  = "myregistry.azurecr.io"
image_tag           = "dc43/governance:latest"
backend_token       = "super-secret"
contract_storage    = "/contracts"
```

Apply the plan:

```bash
terraform -chdir=deploy/terraform/azure-service-backend init
terraform -chdir=deploy/terraform/azure-service-backend apply
```

Bind Azure Files, Blob Storage, or another persistent volume to `contract_storage` if authors need durable drafts. The module
outputs the HTTPS endpoint your Spark developers will target.

### 2.2 AWS ECS Fargate

The AWS stack under [`deploy/terraform/aws-service-backend/`](../../deploy/terraform/aws-service-backend/) creates an ECS cluster,
Application Load Balancer, Fargate service, and EFS volume for contract drafts.

Sample `terraform.tfvars`:

```hcl
aws_region                       = "eu-west-1"
cluster_name                     = "dc43-governance"
ecr_image_uri                    = "123456789012.dkr.ecr.eu-west-1.amazonaws.com/dc43/governance:latest"
backend_token                    = "super-secret"
contract_filesystem              = "dc43-contracts"
private_subnet_ids               = ["subnet-aaaa", "subnet-bbbb"]
load_balancer_subnet_ids         = ["subnet-cccc", "subnet-dddd"]
service_security_group_id        = "sg-0123456789abcdef0"
load_balancer_security_group_id  = "sg-abcdef0123456789"
certificate_arn                  = "arn:aws:acm:eu-west-1:123456789012:certificate/uuid"
```

Apply the plan:

```bash
terraform -chdir=deploy/terraform/aws-service-backend init
terraform -chdir=deploy/terraform/aws-service-backend apply
```

The module exposes the HTTPS listener URL after provisioning. Mount the generated EFS volume inside your pipelines if they need to
share draft artifacts with the backends.

## 4. Handoff to developers

Once the service is reachable:

1. Share the base URL (for example `https://governance.example.com`) and the `DC43_BACKEND_TOKEN` secret with developers.
2. Clarify which storage backend powers the contract store so developers can replicate it locally for tests.
3. Point teams to the [Spark developer guides](spark-local.md) and [remote service walkthrough](spark-remote.md).

Keep your Terraform state in version control or a remote backend (Azure Storage, S3) so future updates stay reproducible.
