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

resource "azurerm_resource_group" "main" {
  name     = var.resource_group_name
  location = var.location
}

resource "random_string" "storage_suffix" {
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
  name                     = lower(substr(replace("${var.resource_group_name}${random_string.storage_suffix.result}", "-", ""), 0, 24))
  location                 = azurerm_resource_group.main.location
  resource_group_name      = azurerm_resource_group.main.name
  account_kind             = "StorageV2"
  account_tier             = "Standard"
  account_replication_type = "LRS"
  enable_https_traffic_only = true
}

resource "azurerm_storage_share" "contracts" {
  name                 = var.contract_share_name
  storage_account_name = azurerm_storage_account.contracts.name
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

  secret {
    name  = "contracts-storage-key"
    value = azurerm_storage_account.contracts.primary_access_key
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

      env {
        name  = "DC43_CONTRACT_STORE"
        value = var.contract_storage
      }

      volume_mounts {
        name      = "contracts"
        mount_path = var.contract_storage
      }
    }

    volume {
      name                                = "contracts"
      storage_type                         = "AzureFile"
      storage_name                         = azurerm_storage_account.contracts.name
      storage_account_name                 = azurerm_storage_account.contracts.name
      share_name                           = azurerm_storage_share.contracts.name
      storage_account_access_key_secret_ref = "contracts-storage-key"
    }
  }

  tags = var.tags
}

output "container_app_fqdn" {
  value = azurerm_container_app.main.latest_revision_fqdn
}

output "storage_account_name" {
  value = azurerm_storage_account.contracts.name
}
