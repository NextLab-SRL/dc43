# Azure ACR setup for dc43 Docker publishing

This guide walks through provisioning an Azure Container Registry (ACR) and the GitHub configuration that the release and CI workflows expect when building and pushing the dc43 Docker images to Azure.

## 1. Create the ACR Registry

Provision an Azure Container Registry in your Azure subscription. You can use the Azure Portal, Azure CLI, or Terraform. Ensure you create a registry name (e.g., `dc43registry`) which will result in a login server like `dc43registry.azurecr.io`.

The workflows will push images for the two backend services:
- Contracts UI (`deploy/contracts-app/Dockerfile` pushed to `<registry-name>.azurecr.io/dc43-contracts-app`)
- HTTP service backends (`deploy/http-backend/Dockerfile` pushed to `<registry-name>.azurecr.io/dc43-http-backend`)

The registry must allow pushes for arbitrary semantic tags. The release workflow publishes each image using the package version reported by the corresponding tag (for example `1.4.0`) and also updates a mutable `latest` tag.

## 2. Configure Azure Active Directory (Entra ID) with GitHub OIDC

For secure passwordless authentication from GitHub Actions to Azure, configure OpenID Connect (OIDC).

1. **Create an App Registration (Service Principal):** In the Azure Portal, navigate to Entra ID > App registrations and create a new application.
2. **Assign the AcrPush Role:** Navigate to your ACR resource in the portal. Under "Access control (IAM)", add a role assignment. Assign the `AcrPush` role to the Service Principal you just created.
3. **Configure Federated Credentials:** Go back to your App registration, under "Certificates & secrets", select "Federated credentials". Add a new credential using the "GitHub Actions deploying Azure resources" scenario.
   - Set the Organization and Repository to your GitHub repository (e.g., `NextLab-SRL/dc43`).
   - Entity type should be "Environment", "Branch", "Pull request", or "Tag" depending on your security posture. For the main release workflow, limit access to the `release` environment or the `main` branch.

## 3. Add GitHub repository secrets

Add the following repository secrets to your GitHub repository so the workflows can authenticate:

- `AZURE_CLIENT_ID` – The Client ID (Application ID) of your Entra ID App registration.
- `AZURE_TENANT_ID` – The Directory (Tenant) ID of your Entra ID tenant.
- `AZURE_SUBSCRIPTION_ID` – The ID of your Azure subscription containing the ACR.
- `AZURE_ACR_REGISTRY_NAME` – The short name of your ACR registry (e.g., `dc43registry`). The workflows will automatically append `.azurecr.io`.
- `PYPI_TOKEN` – Still required for Python package publishing during releases.

## 4. Configure the release environment

The release workflow runs in the `release` environment. Configure it in GitHub (Settings > Environments) with the required reviewers or deployment protections so Docker publishing only happens after manual approval. Ensure your Federated Credentials in Azure correspond to this `release` environment if restricted.

## 5. Validate with the CI smoke test

*(Note: The workflow must be adapted to use the `azure/login` and `azure/docker-login` actions before this will function properly. The following documents the expected behavior once implemented.)*

Use the **Run workflow** button on the `ci` workflow to trigger a manual run with Docker publishing enabled:

1. Open **Actions → ci → Run workflow**.
2. Enable **Publish Docker images to Amazon ECR** (or the generic equivalent once renamed).
3. Optionally supply a custom image tag (defaults to the selected commit SHA).
4. Dispatch the workflow and monitor the `Docker image smoke publish` job. It builds both Dockerfiles and pushes the results to the configured ACR repository, mirroring the release behavior without waiting for a tagged release.

Once the smoke test succeeds, your release pipeline will publish both images for future releases automatically.
