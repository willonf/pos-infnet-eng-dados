# Terraform — Bucket S3 (aula AWS Glue)

Cria um bucket S3 com versionamento, criptografia SSE-S3 e bloqueio de acesso público.

## 1. Pré-requisitos

```bash
brew install terraform
brew install awscli
terraform -version
aws --version
```

## 2. Configurar credenciais da AWS (um único perfil)

Crie um usuário IAM com permissão de administrador **somente para o laboratório**
(ex.: política `AmazonS3FullAccess` + `IAMReadOnlyAccess`), gere um Access Key
e configure o AWS CLI:

```bash
aws configure --profile pos-infnet
# AWS Access Key ID: <sua key>
# AWS Secret Access Key: <seu secret>
# Default region name: us-east-1
# Default output format: json

aws sts get-caller-identity --profile pos-infnet   # valida as credenciais
```

### E se eu usar o usuário root?

Funciona, mas **não é recomendado**: a root key dá acesso total e irrestrito à conta
(não dá para limitar por política, permission boundary ou SCP), não tem MFA e, se vazar,
o prejuízo é a conta inteira — inclusive fatura. Prefira um usuário IAM com
`AdministratorAccess`.

Se for inevitável (conta pessoal de laboratório), o caminho é:

1. Console AWS → menu da conta (canto superior direito) → **Security credentials**
2. **Access keys** → *Create access key* → escolha *Command Line Interface (CLI)*,
   aceite o aviso de que é uma prática não recomendada
3. Guarde o Secret (aparece uma única vez) e configure um profile separado:

```bash
aws configure --profile root
export AWS_PROFILE=root      # ou use -var="aws_profile=root"
```

4. Ao terminar a aula: **desative e apague a access key** em Security credentials e
   ative MFA na conta root.

## 3. Configurar múltiplos perfis (mais de uma conta AWS)

Use esta seção se você alterna entre contas — por exemplo **pessoal** e **INFNET**.
Cada profile do AWS CLI aponta para **uma conta + um usuário**, e você pode ter quantos
quiser no mesmo computador.

### 3.1 Criar os perfis

```bash
aws configure --profile pessoal
# Access Key ID / Secret / região / output

aws configure --profile infnet
# Access Key ID / Secret / região / output
```

Sem modo interativo:

```bash
aws configure set aws_access_key_id     "AKIA..."        --profile pessoal
aws configure set aws_secret_access_key "..."            --profile pessoal
aws configure set region                "us-east-1"      --profile pessoal
aws configure set output                "json"           --profile pessoal
```

Sugestão de nomes: `pessoal`, `infnet`, `root`, `default`. Evite nomes com
espaços ou acentos — eles viram `AWS_PROFILE` e argumentos de linha de comando.

### 3.2 Onde ficam os arquivos

`~/.aws/config` — regiões e formato de saída:

```ini
[profile pessoal]
region = us-east-1
output = json

[profile infnet]
region = us-east-1
output = json
```

`~/.aws/credentials` — **somente access keys** (nunca versione este arquivo,
nunca cole em chat/issue):

```ini
[pessoal]
aws_access_key_id     = AKIA...
aws_secret_access_key = ...

[infnet]
aws_access_key_id     = AKIA...
aws_secret_access_key = ...
```

### 3.3 Conferir os perfis existentes

```bash
aws configure list-profiles          # lista os nomes
aws configure list --profile infnet  # mostra região e ORIGEM da credencial
aws sts get-caller-identity --profile infnet
```

O `get-caller-identity` é o teste definitivo: ele mostra o `Account` (ID da conta) e o
`Arn` (usuário/role). Se o `Account` não for o esperado, você está no profile errado.

### 3.4 Alternar entre perfis

```bash
# 1) por comando
aws s3 ls --profile infnet

# 2) por variável de ambiente (vale para a sessão do terminal)
export AWS_PROFILE=infnet
aws s3 ls
terraform apply

# 3) por variável do Terraform (não mexe no ambiente)
terraform apply -var="aws_profile=pessoal"
```

Precedência: `--profile` na linha de comando > `AWS_PROFILE` > `default`.
No Terraform, se `aws_profile` estiver preenchido (via `-var` ou `terraform.tfvars`),
ele **ignora** o `AWS_PROFILE`. Com `aws_profile = ""` (padrão do `main.tf`), o provider
usa a cadeia do AWS CLI — ou seja, respeita `AWS_PROFILE`.

Para não errar: escolha **um** estilo. O mais simples é deixar `aws_profile` vazio e
trocar tudo com `export AWS_PROFILE=...`.

### 3.5 O state também precisa ser separado por conta

O `terraform.tfstate` é **local e único** para o diretório. Se você aplicar na conta
pessoal, trocar o profile e aplicar de novo no mesmo diretório, o Terraform acha que o
bucket da outra conta ainda existe e não cria nada. Use um workspace por conta:

```bash
terraform workspace new pessoal
terraform workspace new infnet

terraform workspace select infnet
export AWS_PROFILE=infnet
terraform apply -var="expected_account_id=123456789012"
```

Cada workspace guarda seu próprio state em `.terraform.tfstate.d/<workspace>/`.
Para ver onde você está: `terraform workspace show`.

### 3.6 Trava de segurança contra aplicar na conta errada

```hcl
# terraform.tfvars
expected_account_id = "123456789012"
```

Se a conta autenticada for outra, o `apply` falha na `precondition` do bucket antes de
criar qualquer recurso. Confira o resultado com `terraform output aws_account_id`.

> O nome do bucket S3 é **globalmente único**: se a conta pessoal já criou
> `meu-bucket`, a INFNET não consegue criar o mesmo nome. Deixe `name` vazio no mapa
> de `buckets` para o Terraform gerar um sufixo aleatório, ou use nomes distintos
> por conta.

## 4. Rodar o Terraform

```bash
cd 03-aws-glue/terraform

export AWS_PROFILE=pos-infnet       # ou o profile da conta que você quer usar
terraform init                      # baixa o provider AWS
terraform fmt                       # formata o código
terraform validate                  # valida a sintaxe
terraform plan                      # usa AWS_PROFILE / profile do tfvars
terraform apply
```

Para escolher a região e os nomes dos buckets:

```bash
terraform apply -var="aws_region=sa-east-1" \
                -var='buckets={"bronze":{"name":"pos-infnet-bronze-2026"}}'
```

Ou crie um arquivo `terraform.tfvars` (não versionado):

```hcl
aws_profile         = "infnet"         # nome do profile em ~/.aws/credentials
aws_region          = "us-east-1"
expected_account_id = "123456789012"   # opcional, trava de segurança

buckets = {
  bronze = {}                          # nome e force_destroy padrão
  silver = {}
  gold   = { force_destroy = false }   # protege o bucket de destruição acidental
  temp   = {
    name          = "pos-infnet-temp-2026"  # nome fixo (precisa ser globalmente único)
    tags          = { Owner = "seu-nome" }
  }
}
```

`aws_profile` recebe o **nome** do profile (o mesmo de `aws configure --profile infnet`),
não o caminho do arquivo nem a access key. Se preenchido aqui, ele tem prioridade sobre
o `AWS_PROFILE` exportado no terminal; se ficar vazio, o provider usa a cadeia padrão
do AWS CLI.

### 4.1 Adicionar ou remover buckets

Cada chave do mapa `buckets` é uma camada. Para criar mais um bucket, basta acrescentar
uma entrada e rodar `terraform plan`:

```hcl
buckets = {
  bronze = {}
  silver = {}
  gold   = {}
  logs   = {}
}
```

Para remover, apague a entrada. O bucket correspondente será destruído no `apply`
(respeitando `force_destroy`; com `false`, o `destroy` falha se ainda houver objetos).
Versionamento, criptografia, bloqueio público e lifecycle são aplicados automaticamente
a todos os buckets.

> **Se você já aplicou a versão anterior do `main.tf`** (um bucket só, com
> `aws_s3_bucket.this` sem chave), o endereço do resource mudou para
> `aws_s3_bucket.this["bronze"]`, e o Terraform vai tentar recriar tudo. Em laboratório,
> o mais simples é `terraform destroy` antes de aplicar. Para preservar o bucket,
> use `terraform state mv 'aws_s3_bucket.this' 'aws_s3_bucket.this["bronze"]'`
> (e o mesmo para `versioning`, `server_side_encryption_configuration`,
> `public_access_block` e `lifecycle_configuration`).

Depois basta `terraform plan` / `terraform apply`.

## 5. Conferir o resultado

```bash
aws s3 ls --profile infnet
terraform output
terraform output bucket_names
terraform output aws_account_id
```

`bucket_names`, `bucket_arns` e `bucket_regions` são mapas indexados pela mesma chave
usada em `var.buckets`.

## 6. Destruir os recursos

```bash
terraform destroy
```

> `force_destroy = true` (padrão de cada bucket) permite apagar mesmo com objetos
> dentro. Em produção, use `false` e esvazie o bucket antes.

## Alternativa: credenciais só por variáveis de ambiente

```bash
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
export AWS_SESSION_TOKEN="..."   # apenas se a credencial for temporária
export AWS_REGION="us-east-1"

terraform apply
```
