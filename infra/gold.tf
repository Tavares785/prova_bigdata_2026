# gold.tf — CAMADA GOLD  ✍️ construída pelo aluno
# =============================================================================
# Recursos da camada gold: Bucket_Gold, DynamoDB, Glue Catalog, Athena e
# Glue Job. Aplicados junto com o raw.tf no MESMO terraform apply.
#
# Organização do Bucket_Gold (decisão de arquitetura: prefixos em um só bucket):
#   s3://<gold>/fato_pedidos/     -> fato, Parquet particionado por data_pedido
#   s3://<gold>/dim_cliente/      -> dimensão, Parquet sem partição
#   s3://<gold>/dim_produto/      -> dimensão, Parquet sem partição
#   s3://<gold>/scripts/          -> script PySpark do Glue Job
#   s3://<gold>/athena-results/   -> resultados das consultas do Athena
# Vantagem: um único bucket para criar/destruir. Desvantagem: mistura dado
# analítico com artefatos de execução (em produção, costuma-se separar).
# =============================================================================


# -----------------------------------------------------------------------------
# Bucket_Gold — criado via AWS CLI (mesmo padrão do raw.tf)
# -----------------------------------------------------------------------------
# ⚠️ NÃO usar aws_s3_bucket: a SCP do Learner Lab nega
# s3:GetBucketObjectLockConfiguration, que esse recurso sempre consulta.
#
# terraform_data é um recurso "vazio": não representa nada na AWS por si só.
# Ele existe para guardar valores no state (input) e disparar provisioners.
resource "terraform_data" "bucket_gold" {
  # "input" fica salvo no terraform.tfstate. É daqui que o provisioner de
  # destroy lê o nome do bucket (via self.input.bucket), porque provisioners
  # com when = destroy NÃO podem acessar var.* — só self.
  input = {
    bucket = var.bucket_gold_nome
    regiao = var.regiao
  }

  # Se algum valor desta lista mudar, o Terraform DESTRÓI e RECRIA o recurso
  # (roda o provisioner de destroy e depois o de criação de novo).
  # ⚠️ Por isso as tags NÃO entram aqui: mudar uma tag apagaria o bucket
  # inteiro com os dados. Tags ficam num recurso separado (abaixo).
  triggers_replace = [var.bucket_gold_nome, var.regiao]

  # CRIAÇÃO: roda uma vez, quando o recurso é criado.
  provisioner "local-exec" {
    # <<-CMD ... CMD é um "heredoc": texto de várias linhas. O "-" permite
    # indentar o conteúdo (o Terraform remove a indentação comum).
    # ${var.x} dentro dele é interpolação do TERRAFORM, resolvida antes de
    # o shell rodar o comando.
    command = <<-CMD
      set -e

      # head-bucket responde com sucesso só se o bucket existe E temos acesso.
      # Diferença para o raw.tf (que usa "create-bucket ... || true" e engole
      # QUALQUER erro): aqui só pulamos a criação se o bucket já é nosso.
      # Se o nome pertencer a outra conta, head-bucket falha, create-bucket
      # também falha com BucketAlreadyExists e o "set -e" interrompe o apply
      # com uma mensagem clara, em vez de um AccessDenied confuso depois.
      if ! aws s3api head-bucket --bucket "${var.bucket_gold_nome}" 2>/dev/null; then
        # Em us-east-1 não se passa --create-bucket-configuration
        # (LocationConstraint); em outras regiões seria obrigatório.
        aws s3api create-bucket --bucket "${var.bucket_gold_nome}" --region "${var.regiao}"
      fi

      # Bloqueio de acesso público (os 4 controles), igual ao raw:
      #   BlockPublicAcls       -> rejeita novas ACLs públicas
      #   IgnorePublicAcls      -> ignora ACLs públicas que já existam
      #   BlockPublicPolicy     -> rejeita bucket policies que deem acesso público
      #   RestrictPublicBuckets -> restringe acesso mesmo se já houver policy pública
      aws s3api put-public-access-block --bucket "${var.bucket_gold_nome}" \
        --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
    CMD
  }

  # DESTROY: roda no terraform destroy (ou quando triggers_replace força recriar).
  # "rb --force" apaga TODOS os objetos e depois o bucket (um bucket precisa
  # estar vazio para ser removido). "|| true" evita travar o destroy se o
  # bucket já tiver sido removido manualmente.
  provisioner "local-exec" {
    when    = destroy
    command = "aws s3 rb s3://${self.input.bucket} --force || true"
  }
}


# -----------------------------------------------------------------------------
# Tags de custo do Bucket_Gold
# -----------------------------------------------------------------------------
# Separado do bucket de propósito: aqui as tags ESTÃO em triggers_replace, então
# mudar uma tag recria SÓ este recurso (reaplica as tags), sem tocar nos dados.
# Não há provisioner de destroy: ao destruir o bucket, as tags somem com ele.
resource "terraform_data" "bucket_gold_tags" {
  # Referenciar terraform_data.bucket_gold cria uma DEPENDÊNCIA implícita:
  # o Terraform só roda este recurso depois que o bucket existir.
  triggers_replace = [terraform_data.bucket_gold.id, var.tags]

  provisioner "local-exec" {
    # A API espera: {"TagSet":[{"Key":"Projeto","Value":"prova-bigdata"}, ...]}
    # Montamos isso a partir do map var.tags:
    #   [for k, v in var.tags : {...}] -> percorre cada par chave/valor do map
    #   jsonencode(...)                -> converte a estrutura em texto JSON
    # As aspas simples protegem o JSON do shell (os valores das tags não
    # devem conter aspas simples).
    command = <<-CMD
      aws s3api put-bucket-tagging --bucket "${var.bucket_gold_nome}" \
        --tagging '${jsonencode({ TagSet = [for k, v in var.tags : { Key = k, Value = v }] })}'
    CMD
  }
}


# -----------------------------------------------------------------------------
# DynamoDB — catálogo NoSQL de metadados das execuções (Aula 04)
# -----------------------------------------------------------------------------
# Cada execução do Glue Job grava UM item aqui, com: execution_id, data_hora,
# dataset, linhas_lidas, linhas_gravadas e status.
#
# Aqui dá para usar o recurso nativo do provider (aws_dynamodb_table): a
# restrição da SCP afeta só o aws_s3_bucket.
resource "aws_dynamodb_table" "execucoes" {
  name = var.ddb_tabela_nome

  # PAY_PER_REQUEST (on-demand): paga por leitura/escrita feita. Sem tráfego,
  # custo ~zero. A alternativa, PROVISIONED, reserva capacidade (RCU/WCU) que
  # é cobrada por hora mesmo sem uso: um risco para o orçamento do Learner Lab.
  billing_mode = "PAY_PER_REQUEST"

  # Chave de partição (partition key / hash key). O DynamoDB usa o hash desse
  # valor para decidir em qual PARTIÇÃO física o item fica: o mesmo princípio
  # de distribuição de dados do HDFS/S3, aplicado a um banco NoSQL.
  # execution_id é único por execução, então os itens se espalham bem entre
  # as partições (sem "partição quente").
  hash_key = "execution_id"

  # ⚠️ Conceito importante: só se declaram aqui os atributos que são CHAVE
  # (da tabela ou de índices). O DynamoDB é schemaless: data_hora, dataset,
  # linhas_lidas etc. NÃO são declarados; cada item pode ter os atributos que
  # quiser. Declarar um atributo que não é chave dá erro no apply.
  attribute {
    name = "execution_id"
    type = "S" # S = String, N = Number, B = Binary
  }

  # Proteção contra exclusão desligada DE PROPÓSITO: com ela ligada, o
  # terraform destroy falha e a tabela fica de pé no fim da prova.
  # (Em produção, o normal é deixar ligada em tabelas importantes.)
  deletion_protection_enabled = false

  # Criptografia em repouso: o DynamoDB SEMPRE criptografa os dados. Sem o
  # bloco server_side_encryption, usa uma chave gerenciada pela AWS (AWS owned
  # key), sem custo. Uma chave KMS própria (CMK) daria mais controle, mas
  # custa e exige permissões de KMS que o Learner Lab pode não liberar.

  tags = var.tags
}


# -----------------------------------------------------------------------------
# Glue Data Catalog — database + tabelas do modelo estrela
# -----------------------------------------------------------------------------
# O Catalog é um METASTORE (o mesmo papel do Hive Metastore no ecossistema
# Hadoop): guarda só METADADOS (nome, colunas, tipos, local no S3, formato).
# Os dados continuam no S3. O Athena consulta o Catalog para saber "onde está
# e como ler" cada tabela. Esse modelo é chamado de "schema-on-read": o esquema
# é aplicado na leitura, não na gravação.
#
# ⚠️ CONTRATO com o Glue Job: os nomes e TIPOS das colunas abaixo precisam
# bater com o que o PySpark grava no Parquet. Se o Spark gravar "quantidade"
# como bigint e aqui estiver int, o Athena falha com HIVE_BAD_DATA.

resource "aws_glue_catalog_database" "gold" {
  name        = var.glue_database_nome
  description = "Camada gold da prova de Big Data (modelo estrela em Parquet)."
}

# --- Fato: fato_pedidos (particionada por data_pedido) ----------------------
resource "aws_glue_catalog_table" "fato_pedidos" {
  name          = "fato_pedidos"
  database_name = aws_glue_catalog_database.gold.name

  # EXTERNAL_TABLE: a tabela só APONTA para os dados no S3. Um DROP TABLE
  # apaga só os metadados, nunca os arquivos (os dados "pertencem" ao S3).
  table_type = "EXTERNAL_TABLE"

  # Coluna de partição. NÃO aparece em storage_descriptor.columns: o valor não
  # está DENTRO dos arquivos Parquet, e sim no NOME DA PASTA
  # (s3://<gold>/fato_pedidos/data_pedido=2026-01-05/part-*.parquet).
  # É o que permite ao Athena ler só as pastas das datas filtradas
  # ("partition pruning"), escaneando e pagando menos.
  # Tipo string: é o nome da pasta; a projeção abaixo interpreta como data.
  partition_keys {
    name = "data_pedido"
    type = "string"
  }

  parameters = {
    EXTERNAL       = "TRUE"
    classification = "parquet"

    # PARTITION PROJECTION: em vez de registrar cada partição no Catalog
    # (via Crawler ou MSCK REPAIR TABLE), o Athena CALCULA quais partições
    # podem existir a partir destas regras. Partições novas gravadas pelo Job
    # ficam visíveis na hora, sem passo extra.
    "projection.enabled"                   = "true"
    "projection.data_pedido.type"          = "date"
    "projection.data_pedido.format"        = "yyyy-MM-dd"
    "projection.data_pedido.range"         = "2026-01-01,NOW" # do início do ano até hoje
    "projection.data_pedido.interval"      = "1"
    "projection.data_pedido.interval.unit" = "DAYS"
    # storage.location.template foi omitido: sem ele, o Athena assume o
    # layout Hive padrão (<location>/data_pedido=<valor>/), que é exatamente
    # o que o Spark grava com partitionBy("data_pedido").
  }

  storage_descriptor {
    # A barra final importa: indica um "diretório" (prefixo) e não um objeto.
    location = "s3://${var.bucket_gold_nome}/fato_pedidos/"

    # Classes Hive que dizem ao Athena como ler/gravar Parquet.
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      # SerDe = Serializer/Deserializer: converte bytes do arquivo <-> linhas.
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "pedido_id"
      type = "string"
    }
    columns {
      name = "cliente_id"
      type = "string"
    }
    columns {
      name = "produto_id"
      type = "string"
    }
    columns {
      name = "preco_unitario"
      type = "double"
    }
    columns {
      name = "quantidade"
      type = "int"
    }
    columns {
      name = "valor_total"
      type = "double"
    }
  }
}

# --- Dimensão: dim_cliente (sem partição — tabela pequena) -------------------
resource "aws_glue_catalog_table" "dim_cliente" {
  name          = "dim_cliente"
  database_name = aws_glue_catalog_database.gold.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL       = "TRUE"
    classification = "parquet"
  }

  storage_descriptor {
    location      = "s3://${var.bucket_gold_nome}/dim_cliente/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "cliente_id"
      type = "string"
    }
    columns {
      name = "cliente_nome"
      type = "string"
    }
    columns {
      name = "cliente_uf"
      type = "string"
    }
  }
}

# --- Dimensão: dim_produto (sem partição — tabela pequena) -------------------
resource "aws_glue_catalog_table" "dim_produto" {
  name          = "dim_produto"
  database_name = aws_glue_catalog_database.gold.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL       = "TRUE"
    classification = "parquet"
  }

  storage_descriptor {
    location      = "s3://${var.bucket_gold_nome}/dim_produto/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "produto_id"
      type = "string"
    }
    columns {
      name = "produto_nome"
      type = "string"
    }
    columns {
      name = "categoria"
      type = "string"
    }
  }
}


# -----------------------------------------------------------------------------
# Athena Workgroup — onde e como as consultas SQL rodam (Aula 06)
# -----------------------------------------------------------------------------
# O Athena é um motor SQL serverless (baseado em Trino/Presto): lê direto do S3
# usando os metadados do Glue Catalog, sem banco de dados ligado. Cobra por
# volume de dados ESCANEADO, por isso Parquet (colunar) + partição = consulta
# mais barata.
#
# Um workgroup agrupa configurações: local dos resultados, criptografia e
# limites de custo. Selecione "prova-bigdata" no console do Athena antes de
# rodar as consultas de referência.
resource "aws_athena_workgroup" "prova" {
  name        = "prova-bigdata"
  description = "Workgroup da prova de Big Data: consultas sobre a camada gold."

  # Permite ao terraform destroy apagar o workgroup mesmo que ele tenha
  # consultas salvas (named queries). Sem isso, o destroy pode falhar.
  force_destroy = true

  configuration {
    # Obriga TODA consulta deste workgroup a usar as configurações abaixo:
    # quem usa o console não consegue trocar o local de resultados nem
    # desligar a criptografia na mão. Segurança por padrão.
    enforce_workgroup_configuration = true

    # Guardrail de CUSTO: cancela qualquer consulta que tente escanear mais
    # que 10 MB (o mínimo permitido pela AWS). Nosso gold tem poucos KB, então
    # nenhuma consulta legítima chega perto. Mas um SELECT acidental numa
    # tabela enorme seria barrado antes de gastar o orçamento.
    bytes_scanned_cutoff_per_query = 10485760 # 10 * 1024 * 1024 bytes

    result_configuration {
      # Cada consulta grava o resultado (CSV) + metadados neste prefixo.
      # ⚠️ Fica FORA dos prefixos das tabelas (fato_pedidos/, dim_*/) de
      # propósito: se ficasse dentro, o Athena tentaria ler os CSVs de
      # resultado como se fossem dados da tabela e quebraria as consultas.
      output_location = "s3://${var.bucket_gold_nome}/athena-results/"

      # Criptografa os resultados com SSE-S3 (chave gerenciada pelo S3, sem
      # custo). Resultados de consulta podem conter dados sensíveis.
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  tags = var.tags
}


# -----------------------------------------------------------------------------
# Upload do script PySpark para s3://<gold>/scripts/
# -----------------------------------------------------------------------------
# O Glue não lê o script do seu computador: ele baixa do S3 a cada execução.
# Usamos o mesmo padrão terraform_data + AWS CLI dos buckets.
locals {
  # path.module = pasta deste .tf (infra/). O script fica em ../glue-job/.
  script_local = "${path.module}/../glue-job/normaliza_pedidos.py"
  script_s3    = "s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py"
}

resource "terraform_data" "script_glue" {
  # filemd5() calcula o hash MD5 do CONTEÚDO do arquivo. Qualquer alteração no
  # .py muda o hash, o que força a recriação deste recurso e, portanto, um novo
  # upload no próximo apply. Sem isso, o Terraform não teria como saber que o
  # arquivo mudou e o Glue continuaria rodando a versão antiga.
  # O id do bucket entra para reenviar o script se o bucket for recriado.
  triggers_replace = [
    filemd5(local.script_local),
    terraform_data.bucket_gold.id,
  ]

  provisioner "local-exec" {
    command = "aws s3 cp \"${local.script_local}\" \"${local.script_s3}\""
  }
  # Sem provisioner de destroy: o script some junto com o bucket (rb --force).
}


# -----------------------------------------------------------------------------
# Glue Job — normalização raw -> gold em PySpark
# -----------------------------------------------------------------------------
resource "aws_glue_job" "normaliza_pedidos" {
  # Mesmo nome usado nos comandos do README (aws glue start-job-run --job-name ...).
  name        = "normaliza-pedidos"
  description = "Normaliza pedidos_desnormalizado (raw) em fato + dimensões Parquet (gold)."

  # LabRole referenciada por ARN: no Learner Lab não podemos criar roles.
  # É com as permissões DESTA role que o job lê o raw, grava no gold e escreve
  # no DynamoDB (não com as suas credenciais de terminal).
  role_arn = var.labrole_arn

  # Glue 5.0 = Spark 3.5 + Python 3.11, a versão mais próxima do teste local
  # (PySpark 3.5.1), o que reduz surpresas entre testar local e rodar na AWS.
  # Se o Learner Lab recusar, use "4.0" (Spark 3.3 + Python 3.10).
  glue_version = "5.0"

  # --- Custo ------------------------------------------------------------------
  # G.1X = 1 DPU por worker (4 vCPU, 16 GB). 2 workers é o MÍNIMO para glueetl.
  # O Glue cobra por DPU-hora, por segundo, com mínimo de 1 minuto por execução.
  # Nosso dataset tem ~110 linhas: mais workers só aumentariam o custo.
  worker_type       = "G.1X"
  number_of_workers = 2

  # Timeout em MINUTOS. O padrão da AWS é 2880 (48 horas!). Um job travado
  # consumiria orçamento por dois dias. 10 minutos sobra para este volume.
  timeout = 10

  # Sem novas tentativas automáticas: se falhar, falha uma vez só e você
  # investiga o log, em vez de pagar a mesma falha várias vezes.
  max_retries = 0

  execution_property {
    # No máximo uma execução por vez (evita duas rodadas gravando no gold
    # ao mesmo tempo por um clique duplo).
    max_concurrent_runs = 1
  }

  command {
    name            = "glueetl" # job Spark (batch). "pythonshell" seria Python puro, sem Spark.
    script_location = local.script_s3
    python_version  = "3"
  }

  # Argumentos que o script lê com getResolvedOptions(). O nome precisa bater
  # EXATAMENTE (com "--" na frente aqui, sem "--" no script).
  # São os valores PADRÃO: dá para sobrescrever no start-job-run --arguments.
  default_arguments = {
    "--RAW_PATH"     = "s3://${var.bucket_raw_nome}/pedidos/"
    "--GOLD_PATH"    = "s3://${var.bucket_gold_nome}/"
    "--DDB_TABLE"    = aws_dynamodb_table.execucoes.name # referência = dependência implícita
    "--DATASET_NAME" = "pedidos_desnormalizado"

    # Parâmetros especiais do próprio Glue:
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-disable" # sem controle incremental: reprocessa tudo a cada run
    "--enable-continuous-cloudwatch-log" = "true"                 # logs em tempo real no CloudWatch, essencial para depurar
  }

  # script_location é só um texto: o Terraform não sabe que ele depende do
  # upload. depends_on declara essa dependência EXPLICITAMENTE, garantindo que
  # o script já esteja no S3 quando o job for criado.
  depends_on = [terraform_data.script_glue]

  tags = var.tags
}
