variable "subscription_id" {
  description = "Azure subscription ID"
  type        = string
}

variable "resource_group_name" {
  description = "Resource group for the deployment"
  type        = string
}

variable "location" {
  description = "Azure region"
  type        = string
}

variable "container_registry" {
  description = "Azure Container Registry login server"
  type        = string
}

variable "container_registry_username" {
  description = "Username used to pull the container image"
  type        = string
}

variable "container_registry_password" {
  description = "Password or token for the container registry"
  type        = string
  sensitive   = true
}

variable "image_tag" {
  description = "Repository and tag of the container image"
  type        = string
}

variable "backend_token" {
  description = "Bearer token enforced by the service backends"
  type        = string
  default     = ""
}

variable "contract_store_mode" {
  description = "Contract store backing implementation (filesystem or sql)"
  type        = string
  default     = "filesystem"

  validation {
    condition     = contains(["filesystem", "sql"], lower(var.contract_store_mode))
    error_message = "contract_store_mode must be either 'filesystem' or 'sql'."
  }
}

variable "contract_storage" {
  description = "Path mounted inside the container for contracts (filesystem mode only)"
  type        = string
  default     = "/contracts"
}

variable "contract_share_name" {
  description = "Azure Files share used for contract storage when contract_store_mode = 'filesystem'"
  type        = string
  default     = "contracts"
}

variable "contract_share_quota_gb" {
  description = "Quota for the Azure Files share when contract_store_mode = 'filesystem'"
  type        = number
  default     = 100
}

variable "contract_store_dsn" {
  description = "SQLAlchemy DSN used when contract_store_mode = 'sql'"
  type        = string
  default     = ""
  sensitive   = true
}

variable "contract_store_table" {
  description = "Contracts table name when using the SQL store"
  type        = string
  default     = "contracts"
}

variable "contract_store_schema" {
  description = "Optional schema/namespace used by the SQL contract store"
  type        = string
  default     = ""
}

variable "container_app_environment_name" {
  description = "Name of the Container Apps environment"
  type        = string
  default     = "dc43-env"
}

variable "container_app_name" {
  description = "Name of the Container App"
  type        = string
  default     = "dc43-service-backends"
}

variable "ingress_port" {
  description = "Port exposed by the container"
  type        = number
  default     = 8001
}

variable "min_replicas" {
  description = "Minimum number of container replicas"
  type        = number
  default     = 1
}

variable "max_replicas" {
  description = "Maximum number of container replicas"
  type        = number
  default     = 3
}

variable "container_cpu" {
  description = "CPU allocated to the container"
  type        = string
  default     = "0.5"
}

variable "container_memory" {
  description = "Memory allocated to the container"
  type        = string
  default     = "1.0Gi"
}

variable "tags" {
  description = "Tags applied to Azure resources"
  type        = map(string)
  default     = {}
}
