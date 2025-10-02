# Azure Container Apps deployment

This Terraform template deploys the dc43 service backends as a container app.
It provisions:

- A resource group
- Log Analytics workspace for diagnostics
- A Container Apps environment
- A storage account and Azure Files share for the contract store
- The container app with ingress enabled and registry credentials configured

## Usage

1. Build and push the `dc43-service-backends-http` image to an Azure Container
   Registry that the deployment can reach.
2. Create a `terraform.tfvars` file with the variables described below.
3. Run Terraform from the repository root:

   ```bash
   terraform -chdir=deploy/terraform/azure-service-backend init
   terraform -chdir=deploy/terraform/azure-service-backend apply
   ```

## Required variables

| Name | Description |
| ---- | ----------- |
| `subscription_id` | Azure subscription that hosts the deployment. |
| `resource_group_name` | Name of the resource group that will be created (or reused). |
| `location` | Azure region (e.g. `westeurope`). |
| `container_registry` | Login server of the Azure Container Registry (e.g. `myregistry.azurecr.io`). |
| `container_registry_username` | Username for pulling the image. |
| `container_registry_password` | Password or token for the registry. |
| `image_tag` | Repository and tag of the container image (e.g. `dc43-service-backends-http:latest`). |
| `backend_token` | Optional bearer token enforced by the service backends. |
| `contract_storage` | Mount path used by the service to persist contracts. |

Optional variables let you customise the container app name, ingress port, and
scaling settings—see `variables.tf` for the full list.

## Outputs

- `container_app_fqdn` – Public FQDN of the container app.
- `storage_account_name` – Azure Storage account that holds the contracts share.

Use the storage account to upload existing contracts or to back up drafts.
