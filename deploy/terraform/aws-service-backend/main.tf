terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.20.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

resource "aws_ecs_cluster" "main" {
  name = var.cluster_name
}

resource "aws_cloudwatch_log_group" "main" {
  name              = "/dc43/service-backends"
  retention_in_days = var.log_retention_days
}

resource "aws_iam_role" "task_execution" {
  name = "${var.cluster_name}-task-execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "task_execution" {
  role       = aws_iam_role.task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "task" {
  name = "${var.cluster_name}-task"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "task_efs" {
  name = "${var.cluster_name}-efs-access"
  role = aws_iam_role.task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "elasticfilesystem:ClientMount",
          "elasticfilesystem:ClientWrite",
          "elasticfilesystem:ClientRootAccess"
        ]
        Resource = aws_efs_file_system.contracts.arn
      }
    ]
  })
}

resource "aws_security_group" "efs" {
  name        = "${var.cluster_name}-efs"
  description = "Allow NFS from ECS tasks"
  vpc_id      = var.vpc_id

  ingress {
    from_port       = 2049
    to_port         = 2049
    protocol        = "tcp"
    security_groups = [var.service_security_group_id]
    description     = "NFS from ECS tasks"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_efs_file_system" "contracts" {
  creation_token = var.contract_filesystem
  encrypted      = true
}

resource "aws_efs_mount_target" "contracts" {
  for_each = toset(var.private_subnet_ids)

  file_system_id  = aws_efs_file_system.contracts.id
  subnet_id       = each.value
  security_groups = [aws_security_group.efs.id]
}

resource "aws_lb" "main" {
  name               = "${var.cluster_name}-alb"
  load_balancer_type = "application"
  security_groups    = [var.load_balancer_security_group_id]
  subnets            = var.load_balancer_subnet_ids
  idle_timeout       = 60
}

resource "aws_lb_target_group" "main" {
  name        = "${var.cluster_name}-tg"
  port        = var.container_port
  protocol    = "HTTP"
  target_type = "ip"
  vpc_id      = var.vpc_id

  health_check {
    path                = var.health_check_path
    matcher             = "200-399"
    healthy_threshold   = var.health_check_healthy_threshold
    unhealthy_threshold = var.health_check_unhealthy_threshold
    interval            = var.health_check_interval
    timeout             = var.health_check_timeout
  }
}

resource "aws_lb_listener" "https" {
  load_balancer_arn = aws_lb.main.arn
  port              = 443
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"
  certificate_arn   = var.certificate_arn

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.main.arn
  }
}

resource "aws_ecs_task_definition" "main" {
  family                   = "${var.cluster_name}-service"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.task_cpu
  memory                   = var.task_memory
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task.arn

  volume {
    name = "contracts"

    efs_volume_configuration {
      file_system_id     = aws_efs_file_system.contracts.id
      transit_encryption = "ENABLED"
    }
  }

  container_definitions = jsonencode([
    {
      name         = "dc43-service-backends"
      image        = var.ecr_image_uri
      essential    = true
      portMappings = [{
        containerPort = var.container_port
        hostPort      = var.container_port
        protocol      = "tcp"
      }]
      environment = [
        {
          name  = "DC43_BACKEND_TOKEN"
          value = var.backend_token
        },
        {
          name  = "DC43_CONTRACT_STORE"
          value = var.contract_storage_path
        }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.main.name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "dc43"
        }
      }
      mountPoints = [{
        containerPath = var.contract_storage_path
        sourceVolume  = "contracts"
        readOnly      = false
      }]
    }
  ])
}

resource "aws_ecs_service" "main" {
  name            = "${var.cluster_name}-service"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.main.arn
  desired_count   = var.desired_count
  launch_type     = "FARGATE"
  deployment_minimum_healthy_percent = 50
  deployment_maximum_percent         = 200

  network_configuration {
    subnets         = var.private_subnet_ids
    security_groups = [var.service_security_group_id]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.main.arn
    container_name   = "dc43-service-backends"
    container_port   = var.container_port
  }

  lifecycle {
    ignore_changes = [desired_count]
  }

  depends_on = [aws_lb_listener.https]
}

output "load_balancer_dns_name" {
  value = aws_lb.main.dns_name
}

output "service_name" {
  value = aws_ecs_service.main.name
}

output "efs_id" {
  value = aws_efs_file_system.contracts.id
}
