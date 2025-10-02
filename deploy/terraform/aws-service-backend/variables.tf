variable "aws_region" {
  description = "AWS region"
  type        = string
}

variable "cluster_name" {
  description = "ECS cluster name"
  type        = string
}

variable "ecr_image_uri" {
  description = "Fully qualified ECR image URI"
  type        = string
}

variable "backend_token" {
  description = "Bearer token enforced by the service"
  type        = string
  default     = ""
}

variable "contract_filesystem" {
  description = "Identifier for the EFS file system"
  type        = string
}

variable "contract_storage_path" {
  description = "Mount path inside the container for the contract store"
  type        = string
  default     = "/contracts"
}

variable "private_subnet_ids" {
  description = "Private subnet IDs for the ECS service"
  type        = list(string)
}

variable "load_balancer_subnet_ids" {
  description = "Subnets for the Application Load Balancer"
  type        = list(string)
}

variable "service_security_group_id" {
  description = "Security group applied to ECS tasks"
  type        = string
}

variable "load_balancer_security_group_id" {
  description = "Security group attached to the load balancer"
  type        = string
}

variable "certificate_arn" {
  description = "ACM certificate for HTTPS"
  type        = string
}

variable "vpc_id" {
  description = "VPC hosting the deployment"
  type        = string
}

variable "task_cpu" {
  description = "Fargate CPU units"
  type        = string
  default     = "512"
}

variable "task_memory" {
  description = "Fargate memory in MiB"
  type        = string
  default     = "1024"
}

variable "container_port" {
  description = "Container listening port"
  type        = number
  default     = 8001
}

variable "desired_count" {
  description = "Number of task replicas"
  type        = number
  default     = 2
}

variable "health_check_path" {
  description = "ALB health check path"
  type        = string
  default     = "/health"
}

variable "health_check_interval" {
  description = "Seconds between health checks"
  type        = number
  default     = 30
}

variable "health_check_timeout" {
  description = "Health check timeout"
  type        = number
  default     = 5
}

variable "health_check_healthy_threshold" {
  description = "Healthy threshold"
  type        = number
  default     = 2
}

variable "health_check_unhealthy_threshold" {
  description = "Unhealthy threshold"
  type        = number
  default     = 2
}

variable "log_retention_days" {
  description = "CloudWatch log retention"
  type        = number
  default     = 30
}
