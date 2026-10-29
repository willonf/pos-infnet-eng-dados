terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  # Se você usa perfis nomeados do AWS CLI (aws configure --profile <nome>),
  # informe o nome em var.aws_profile. Se usa variáveis de ambiente
  # (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY), deixe vazio.
  profile = var.aws_profile != "" ? var.aws_profile : null
}

############################
# Variáveis
############################

variable "aws_region" {
  description = "Região da AWS onde os buckets serão criados"
  type        = string
  default     = "us-east-1"
}

variable "aws_profile" {
  description = "Perfil do AWS CLI a ser usado. Vazio = credenciais via variáveis de ambiente"
  type        = string
  default     = ""
}

variable "buckets" {
  description = "Buckets a criar, indexados por um nome curto (ex.: bronze, silver, gold)"
  type = map(object({
    name          = optional(string, "") # nome fixo; vazio = nome gerado automaticamente
    force_destroy = optional(bool, true) # apaga o bucket mesmo com objetos dentro
    tags          = optional(map(string), {})
  }))
  default = {
    bronze = {}
    silver = {}
    gold   = {}
  }
}

variable "project" {
  description = "Prefixo usado quando o nome do bucket é gerado automaticamente"
  type        = string
  default     = "polaris"
}

variable "tags" {
  description = "Tags aplicadas a todos os recursos"
  type        = map(string)
  default = {
    Project     = "pos-infnet-eng-dados"
    Environment = "aula"
    ManagedBy   = "terraform"
  }
}

variable "expected_account_id" {
  description = "Opcional: ID da conta AWS esperada. Protege contra aplicar na conta errada"
  type        = string
  default     = ""
}

# Usado para conferir em qual conta você está autenticado
data "aws_caller_identity" "current" {}

############################
# Buckets S3
############################

resource "aws_s3_bucket" "this" {
  for_each = var.buckets

  # Ou você define o nome completo, ou deixa o Terraform gerar um sufixo único
  bucket        = each.value.name != "" ? each.value.name : null
  bucket_prefix = each.value.name == "" ? "${var.project}-${each.key}-" : null

  force_destroy = each.value.force_destroy

  tags = merge(var.tags, each.value.tags, { Layer = each.key })

  lifecycle {
    precondition {
      condition     = var.expected_account_id == "" || data.aws_caller_identity.current.account_id == var.expected_account_id
      error_message = "Conta autenticada (${data.aws_caller_identity.current.account_id}) diferente de expected_account_id (${var.expected_account_id}). Confira o profile AWS antes do apply."
    }
  }
}

# Versionamento: mantém histórico de cada objeto (útil para camadas bronze/silver)
resource "aws_s3_bucket_versioning" "this" {
  for_each = aws_s3_bucket.this

  bucket = each.value.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Criptografia padrão SSE-S3 (AES256) em todos os objetos
resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = aws_s3_bucket.this

  bucket = each.value.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Bloqueia qualquer acesso público ao bucket
resource "aws_s3_bucket_public_access_block" "this" {
  for_each = aws_s3_bucket.this

  bucket = each.value.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Boa prática: limpa uploads multipart incompletos depois de 7 dias
resource "aws_s3_bucket_lifecycle_configuration" "this" {
  for_each = aws_s3_bucket.this

  bucket = each.value.id

  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

############################
# Outputs
############################

output "bucket_names" {
  description = "Nome de cada bucket criado, por chave"
  value       = { for k, b in aws_s3_bucket.this : k => b.id }
}

output "bucket_arns" {
  description = "ARN de cada bucket criado, por chave"
  value       = { for k, b in aws_s3_bucket.this : k => b.arn }
}

output "bucket_regions" {
  description = "Região de cada bucket criado, por chave"
  value       = { for k, b in aws_s3_bucket.this : k => b.region }
}

output "aws_account_id" {
  description = "Conta AWS em que os recursos foram criados"
  value       = data.aws_caller_identity.current.account_id
}

output "aws_caller_arn" {
  description = "Usuário/role usado pelo Terraform"
  value       = data.aws_caller_identity.current.arn
}
