from pyspark import pipelines as dp
from pyspark.sql.functions import count, countDistinct, avg, round as spark_round

SILVER = "projeto2.silver.projeto2_silver_quality"

# ============================================================
# GOLD 1 — DIMENSÃO LOCALIDADE
# ============================================================

@dp.materialized_view(
    name="dim_localidade",
    comment="Dimensão de cidades e UFs dos alunos"
)
def dim_localidade():
    return (
        spark.read.table(SILVER)
        .select("cidade", "uf")
        .filter("cidade IS NOT NULL AND uf IS NOT NULL")
        .dropDuplicates(["cidade", "uf"])
    )


# ============================================================
# GOLD 2 — DIMENSÃO CURSO
# ============================================================

@dp.materialized_view(
    name="dim_curso",
    comment="Dimensão de cursos, períodos e turmas"
)
def dim_curso():
    return (
        spark.read.table(SILVER)
        .select("curso", "periodo", "turma")
        .filter("curso IS NOT NULL")
        .dropDuplicates(["curso", "periodo", "turma"])
    )


# ============================================================
# GOLD 3 — DIMENSÃO ALUNO (SCD2)
# ============================================================

@dp.materialized_view(
    name="dim_aluno",
    comment="Dimensão de alunos com histórico SCD Tipo 2"
)
def dim_aluno():
    return spark.read.table("projeto2.silver.dim_aluno_scd2")


# ============================================================
# GOLD 4 — FATO MATRÍCULAS
# ============================================================

@dp.materialized_view(
    name="fato_matriculas",
    comment="Matrículas, alunos e desempenho por curso, localidade, status e bolsa"
)
def fato_matriculas():
    return (
        spark.read.table(SILVER)
        .groupBy(
            "data_matricula",
            "curso",
            "periodo",
            "cidade",
            "uf",
            "status",
            "bolsa"
        )
        .agg(
            count("matricula").alias("quantidade_matriculas"),
            countDistinct("cpf").alias("quantidade_alunos"),
            spark_round(avg("nota_final"), 2).alias("media_nota_final"),
            spark_round(avg("frequencia"), 2).alias("media_frequencia")
        )
    )