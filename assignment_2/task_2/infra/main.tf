terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    http = {
      source  = "hashicorp/http"
      version = "~> 3.0"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
    }
  }
}

provider "aws" {
  region     = var.aws_region
  access_key = var.aws_access_key_id
  secret_key = var.aws_secret_access_key
  token      = var.aws_session_token
}

# IP atual da máquina que executa o terraform — restringe acesso à porta 3306
data "http" "myip" {
  url = "https://checkip.amazonaws.com"
}

locals {
  my_cidr = "${chomp(data.http.myip.response_body)}/32"
}

# VPC default (onde o RDS e o Glue ficam)
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# Subnet usada pelo Glue Connection
data "aws_subnet" "glue" {
  id = tolist(data.aws_subnets.default.ids)[0]
}

# Route tables da VPC default (necessário para o VPC endpoint de S3)
data "aws_route_tables" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# VPC Gateway Endpoint para S3 — permite o Glue acessar S3 sem NAT Gateway.
# Opcional: se a VPC default JÁ tem um endpoint S3 (ex.: criado pelo A1), deixe
# manage_s3_endpoint = false para reusar o existente e evitar conflito de rota.
resource "aws_vpc_endpoint" "s3" {
  count             = var.manage_s3_endpoint ? 1 : 0
  vpc_id            = data.aws_vpc.default.id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = data.aws_route_tables.default.ids
}
