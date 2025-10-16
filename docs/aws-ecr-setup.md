# Amazon ECR setup for dc43 Docker publishing

This guide walks through provisioning the Amazon Elastic Container Registry (ECR)
resources and GitHub configuration that the release and CI workflows expect when
building and pushing the dc43 Docker images.

## 1. Create the ECR repositories

Provision two repositories in the AWS account that will host your images:

| Repository purpose | Suggested name | GitHub secret |
| --- | --- | --- |
| Contracts UI (`deploy/contracts-app/Dockerfile`) | `dc43-contracts-app` | `AWS_ECR_CONTRACTS_APP_REPOSITORY` |
| HTTP service backends (`deploy/http-backend/Dockerfile`) | `dc43-http-backend` | `AWS_ECR_HTTP_BACKEND_REPOSITORY` |

Both repositories must allow pushes for arbitrary semantic tags. The workflows
publish an immutable `${GITHUB_SHA}` tag for each run and also update a mutable
`latest` tag.

## 2. Configure IAM with GitHub OIDC

1. [Create an IAM identity provider](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_providers_create_oidc.html)
   targeting `token.actions.githubusercontent.com` if your account does not
   already have one for GitHub Actions.
2. Create an IAM role that trusts this provider. Scope the trust policy to the
   GitHub repository (or environment) that will run the workflows. A minimal
   example condition limits access to the `release` environment:

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Principal": {
           "Federated": "arn:aws:iam::<ACCOUNT_ID>:oidc-provider/token.actions.githubusercontent.com"
         },
         "Action": "sts:AssumeRoleWithWebIdentity",
         "Condition": {
           "StringEquals": {
             "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
             "token.actions.githubusercontent.com:sub": "repo:<OWNER>/<REPO>:environment:release"
           }
         }
       }
     ]
   }
   ```
3. Attach a permissions policy that grants the Docker workflows ECR access. At
   minimum include:

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": [
           "ecr:GetAuthorizationToken",
           "ecr:BatchCheckLayerAvailability",
           "ecr:CompleteLayerUpload",
           "ecr:DescribeImages",
           "ecr:InitiateLayerUpload",
           "ecr:PutImage",
           "ecr:UploadLayerPart"
         ],
         "Resource": "*"
       }
     ]
   }
   ```

   Optionally extend the policy with `ecr:CreateRepository` and
   `ecr:DescribeRepositories` if you want the workflows to provision missing
   repositories automatically.

## 3. Add GitHub repository secrets

Populate the secrets that the workflows reference:

- `AWS_REGION` – AWS region that hosts the ECR repositories.
- `AWS_ROLE_TO_ASSUME` – ARN of the IAM role created above.
- `AWS_ECR_CONTRACTS_APP_REPOSITORY` – name of the contracts app repository.
- `AWS_ECR_HTTP_BACKEND_REPOSITORY` – name of the HTTP backend repository.
- `PYPI_TOKEN` – still required for Python package publishing during releases.

## 4. Configure the release environment

The release workflow runs in the `release` environment. Configure it with the
required reviewers or deployment protections so Docker publishing only happens
after manual approval.

## 5. Validate with the CI smoke test

Use the **Run workflow** button on the `ci` workflow to trigger a manual run
with Docker publishing enabled:

1. Open **Actions → ci → Run workflow**.
2. Enable **Publish Docker images to Amazon ECR**.
3. Optionally supply a custom image tag (defaults to the selected commit SHA).
4. Dispatch the workflow and monitor the `Docker image smoke publish` job. It
   builds both Dockerfiles and pushes the results to the configured ECR
   repositories, mirroring the release behavior without waiting for a tagged
   release.

Once the smoke test succeeds, your release pipeline will publish both images for
future releases automatically.
