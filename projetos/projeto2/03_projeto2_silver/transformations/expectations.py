
from pyspark import pipelines as dp
from pyspark.sql.functions import expr

REGRAS = {

    "matricula_preenchida":
        "matricula IS NOT NULL",

    "nome_preenchido":
        "nome IS NOT NULL",

    "cpf_preenchido":
        "cpf IS NOT NULL",

    "status_valido":
        "status IN ('ativo', 'trancado', 'formado', 'jubilado')",

    "email_preenchido":
        "email IS NOT NULL",

    "curso_preenchido":
        "curso IS NOT NULL"
}


# Condição para identificar registros que devem
# ir para a quarentena
QUARANTINE_CONDITION = (
    "NOT("
    + " AND ".join(REGRAS.values())
    + ")"
)


# ============================================================
# QUALITY / EXPECTATIONS
# ============================================================

@dp.table(
    name="projeto2_quality",
    comment="Registros com regras de qualidade aplicadas"
)
@dp.expect_all(REGRAS)
def projeto2_quality():
    return (
        spark.readStream
            .table("projeto2.bronze.projeto2_bronze")
            .withColumn(
                "is_quarantined",
                expr(QUARANTINE_CONDITION)
            )
    )


# ============================================================
# QUARANTINE
# ============================================================

@dp.table(
    name="projeto2_quarantine",
    comment="Registros que não tiveram sucesso nas regras de qualidade"
)
def projeto2_quarantine():
    return (
        spark.readStream
            .table("projeto2_quality")
            .filter("is_quarantined = true")
    )


# ============================================================
# SILVER QUALITY
# ============================================================

@dp.table(
    name="projeto2_silver_quality",
    comment="Registros validados após as regras de qualidade",
    partition_cols=["uf", "curso"],
    table_properties={
        "pipelines.autoOptimize.zOrderCols": "cpf,cidade"
    }
)
def projeto2_silver_quality():
    return (
        spark.readStream
            .table("projeto2_quality")
            .filter("is_quarantined = false")
            .drop("is_quarantined")
    )