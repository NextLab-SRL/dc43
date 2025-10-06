terraform {
  required_version = ">= 1.5.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 3.72.0"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.5.1"
    }
  }
}

provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}

locals {
  contract_store_mode = lower(var.contract_store_mode)
  use_filesystem      = local.contract_store_mode == "filesystem"
  use_sql             = local.contract_store_mode == "sql"
}

resource "azurerm_resource_group" "main" {
  name     = var.resource_group_name
  location = var.location
}

resource "random_string" "storage_suffix" {
  count   = local.use_filesystem ? 1 : 0
  length  = 6
  upper   = false
  special = false
}

resource "azurerm_log_analytics_workspace" "main" {
  name                = "${var.resource_group_name}-law"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "PerGB2018"
  retention_in_days   = 30
}

resource "azurerm_container_app_environment" "main" {
  name                       = var.container_app_environment_name
  location                   = azurerm_resource_group.main.location
  resource_group_name        = azurerm_resource_group.main.name
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main.id
}

resource "azurerm_storage_account" "contracts" {
  count                    = local.use_filesystem ? 1 : 0
  name                     = lower(substr(replace("${var.resource_group_name}${random_string.storage_suffix[0].result}", "-", ""), 0, 24))
  location                 = azurerm_resource_group.main.location
  resource_group_name      = azurerm_resource_group.main.name
  account_kind             = "StorageV2"
  account_tier             = "Standard"
  account_replication_type = "LRS"
  enable_https_traffic_only = true
}

resource "azurerm_storage_share" "contracts" {
  count                = local.use_filesystem ? 1 : 0
  name                 = var.contract_share_name
  storage_account_name = azurerm_storage_account.contracts[0].name
  quota                = var.contract_share_quota_gb
}

resource "azurerm_container_app" "main" {
  name                         = var.container_app_name
  resource_group_name          = azurerm_resource_group.main.name
  location                     = azurerm_resource_group.main.location
  container_app_environment_id = azurerm_container_app_environment.main.id
  revision_mode                = "Single"

  ingress {
    external_enabled = true
    target_port      = var.ingress_port
    transport        = "Auto"
    traffic_weight {
      latest_revision = true
      percentage      = 100
    }
  }

  registry {
    server               = var.container_registry
    username             = var.container_registry_username
    password_secret_name = "registry-password"
  }

  secret {
    name  = "registry-password"
    value = var.container_registry_password
  }

  dynamic "secret" {
    for_each = local.use_filesystem ? [azurerm_storage_account.contracts[0].primary_access_key] : []
    content {
      name  = "contracts-storage-key"
      value = secret.value
    }
  }

  dynamic "secret" {
    for_each = local.use_sql ? [var.contract_store_dsn] : []
    content {
      name  = "contract-store-dsn"
      value = secret.value
    }
  }

  template {
    min_replicas = var.min_replicas
    max_replicas = var.max_replicas

    container {
      name   = "dc43-service-backends"
      image  = "${var.container_registry}/${var.image_tag}"
      cpu    = var.container_cpu
      memory = var.container_memory

      env {
        name  = "DC43_BACKEND_TOKEN"
        value = var.backend_token
      }

      dynamic "env" {
        for_each = local.use_filesystem ? [var.contract_storage] : []
        content {
          name  = "DC43_CONTRACT_STORE"
          value = env.value
        }
      }

      dynamic "env" {
        for_each = local.use_sql ? [1] : []
        content {
          name  = "DC43_CONTRACT_STORE_TYPE"
          value = "sql"
        }
      }

      dynamic "env" {
        for_each = local.use_sql ? [var.contract_store_table] : []
        content {
          name  = "DC43_CONTRACT_STORE_TABLE"
          value = env.value
        }
      }

      dynamic "env" {
        for_each = local.use_sql && length(trimspace(var.contract_store_schema)) > 0 ? [trimspace(var.contract_store_schema)] : []
        content {
          name  = "DC43_CONTRACT_STORE_SCHEMA"
          value = env.value
        }
      }

      dynamic "env" {
        for_each = local.use_sql ? [1] : []
        content {
          name        = "DC43_CONTRACT_STORE_DSN"
          secret_name = "contract-store-dsn"
        }
      }

      dynamic "volume_mounts" {
        for_each = local.use_filesystem ? [1] : []
        content {
          name       = "contracts"
          mount_path = var.contract_storage
        }
      }
    }

    dynamic "volume" {
      for_each = local.use_filesystem ? [1] : []
      content {
        name                           = "contracts"
        storage_type                    = "AzureFile"
        storage_name                    = azurerm_storage_account.contracts[0].name
        storage_account_name            = azurerm_storage_account.contracts[0].name
        share_name                      = azurerm_storage_share.contracts[0].name
        storage_account_key_secret_name = "contracts-storage-key"
      }
    }
  }

  tags = var.tags

  lifecycle {
    precondition {
      condition     = !local.use_sql || length(trimspace(var.contract_store_dsn)) > 0
      error_message = "contract_store_dsn must be provided when contract_store_mode = 'sql'."
    }
  }
}

output "container_app_fqdn" {
  value = azurerm_container_app.main.latest_revision_fqdn
}

output "storage_account_name" {
  value = local.use_filesystem ? azurerm_storage_account.contracts[0].name : null
}
