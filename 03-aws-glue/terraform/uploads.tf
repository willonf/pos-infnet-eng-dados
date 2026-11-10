############################
# Upload de arquivos de dados para os buckets
############################
#
# Envia, no mesmo `terraform apply` que cria os buckets, os arquivos de uma pasta
# local para um bucket (camada) de var.buckets. Cada arquivo vira um aws_s3_object:
#   - editar o arquivo local => o próximo apply reenvia (detectado pelo hash)
#   - apagar o arquivo local => o próximo apply remove o objeto do bucket
#   - terraform destroy      => remove os objetos junto com os buckets

variable "bucket_uploads" {
  description = "Pastas locais a enviar para os buckets. bucket = chave em var.buckets"
  type = list(object({
    bucket  = string                 # chave em var.buckets (ex.: bronze)
    source  = string                 # pasta local, relativa a este diretório
    pattern = optional(string, "**") # glob dos arquivos dentro de source (ex.: "*.csv")
    prefix  = optional(string, "")   # prefixo no bucket (ex.: "raw/"); vazio = raiz
  }))
  default = [
    {
      bucket = "bronze"
      source = "../../projetos/projeto3/dataset"
    },
  ]
}

locals {
  upload_content_types = {
    csv     = "text/csv"
    json    = "application/json"
    txt     = "text/plain"
    parquet = "application/vnd.apache.parquet"
  }

  # Um item por arquivo, indexado por "<bucket>/<chave no S3>"
  bucket_upload_files = merge([
    for u in var.bucket_uploads : {
      for f in fileset("${path.module}/${u.source}", u.pattern) :
      "${u.bucket}/${u.prefix}${f}" => {
        bucket = u.bucket
        key    = "${u.prefix}${f}"
        path   = "${path.module}/${u.source}/${f}"
      }
      if !endswith(f, ".DS_Store")
    }
  ]...)
}

resource "aws_s3_object" "upload" {
  for_each = local.bucket_upload_files

  bucket      = aws_s3_bucket.this[each.value.bucket].id
  key         = each.value.key
  source      = each.value.path
  source_hash = filemd5(each.value.path)

  content_type = lookup(
    local.upload_content_types,
    lower(regex("[^.]*$", each.value.key)),
    "application/octet-stream",
  )

  tags = var.tags

  # Garante que a criptografia padrão já esteja configurada antes do upload
  depends_on = [aws_s3_bucket_server_side_encryption_configuration.this]
}

output "uploaded_objects" {
  description = "Objetos enviados pelo Terraform: s3://bucket/chave"
  value       = sort([for o in aws_s3_object.upload : "s3://${o.bucket}/${o.key}"])
}
