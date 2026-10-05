from pyspark import pipelines as dp
from pyspark.sql.functions import col


# ============================================================
# IDENTIFICA O PRÓXIMO SNAPSHOT
# ============================================================

def proximo_snapshot(latest_snapshot_version):

    df = spark.read.table(
        "projeto2_silver_quality"
    )

    snapshots = (
        df
        .select("data_matricula")
        .distinct()
        .orderBy("data_matricula")
    )

    # Primeiro snapshot
    if latest_snapshot_version is None:

        rows = (
            snapshots
            .limit(1)
            .collect()
        )

    # Próximo snapshot
    else:

        rows = (
            snapshots
            .filter(
                col("data_matricula")
                > latest_snapshot_version
            )
            .limit(1)
            .collect()
        )

    # Não existe novo snapshot
    if not rows:
        return None

    proxima_data = rows[0]["data_matricula"]

    snapshot = (
        df
        .filter(
            col("data_matricula")
            == proxima_data
        )
    )

    return snapshot, proxima_data


# ============================================================
# 2. SCD TIPO 1
# ============================================================

dp.create_streaming_table(
    name="dim_aluno_scd1",
    comment="Dimensão de alunos - SCD Tipo 1"
)

dp.create_auto_cdc_flow(
    target="dim_aluno_scd1",
    source="projeto2_silver_quality",
    keys=["matricula"],
    sequence_by="ingestion_timestamp",
    except_column_list=["_rescued_data"],
    stored_as_scd_type=1,
)


# ============================================================
# 3. SCD TIPO 2
# ============================================================

dp.create_streaming_table(
    name="dim_aluno_scd2",
    comment="Dimensão de alunos - SCD Tipo 2"
)


dp.create_auto_cdc_flow(
    target="dim_aluno_scd2",
    source="projeto2_silver_quality",
    keys=["matricula"],
    sequence_by="ingestion_timestamp",
    except_column_list=["_rescued_data"],
    track_history_except_column_list=["ingestion_timestamp"],
    stored_as_scd_type=2,
)