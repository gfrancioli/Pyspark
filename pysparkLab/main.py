import os
import sys
import glob
import findspark
from pyspark.sql.functions import col, sum as _sum, count, lit, avg, percentile_approx
from pyspark.sql.types import DecimalType

# 1. Define Paths and Environment Variables
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-8-openjdk-amd64'
os.environ['SPARK_HOME'] = '/content/spark-3.5.8-bin-hadoop3'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['SPARK_CLASSPATH'] = '/content/spark-3.5.8-bin-hadoop3/jars'

# 2. Add PySpark and Py4J to sys.path
spark_home = os.environ['SPARK_HOME']
findspark.init(spark_home)
pyspark_path = os.path.join(spark_home, 'python')
py4j_zip_list = glob.glob(os.path.join(pyspark_path, 'lib', 'py4j-*-src.zip'))

if py4j_zip_list:
    py4j_zip = py4j_zip_list[0]
    if py4j_zip not in sys.path: sys.path.insert(0, py4j_zip)
if pyspark_path not in sys.path: sys.path.insert(0, pyspark_path)


from pyspark.sql import SparkSession

try:
    current_spark = SparkSession.builder.getOrCreate()
    current_spark.stop()
except: pass

spark = SparkSession.builder \
    .master('local[*]') \
    .appName('Correction') \
    .config('spark.driver.bindAddress', '127.0.0.1') \
    .getOrCreate()

try:
    problemas = []
    clients_df = spark.read.json("data-clients.json")
    pedidos_df = spark.read.json("data-pedidos.json")

    valores_nulos = pedidos_df.filter(col("value").isNull())
    for row in valores_nulos.select("id").collect():
        problemas.append({"Id": row.id, "Motivo": "Valor nulo"})

    valores_invalidos = pedidos_df.filter(col("value") <= 0)
    for row in valores_invalidos.select("id", "value").collect():
        motivo = "Valor nulo" if row.value is None else f"Valor inválido (R$ {row.value:.2f})"
        problemas.append({"Id": row.id, "Motivo": motivo})

    cliente_ids_validos = set(clients_df.select("id").rdd.map(lambda r: r.id).collect())
    client_ids_pedidos = pedidos_df.select("client_id").distinct().rdd.map(lambda r: r.client_id).collect()
    ids_invalidos = [cid for cid in client_ids_pedidos if cid not in cliente_ids_validos]

    if ids_invalidos:
        pedidos_invalidos = pedidos_df.filter(col("client_id").isin(ids_invalidos))
        for row in pedidos_invalidos.select("id", "client_id").collect():
            problemas.append({"Id": row.id, "Motivo": "Cliente inválido"})

    ids_duplicados = pedidos_df.groupBy("id").count().filter(col("count") > 1)
    for row in ids_duplicados.select("id").collect():
        problemas.append({"Id": row.id, "Motivo": "ID duplicado"})

    print("\n" + "=" * 60)
    if problemas:
        df_problemas = spark.createDataFrame(problemas)
        print(f"\nTotal de problemas encontrados: {df_problemas.count()}\n")
        resultado_problemas = df_problemas.orderBy("Motivo")
        resultado_problemas.show(200, truncate=False)
        print("-" * 60)
    else:
        print("\n Nenhum problema de qualidade encontrado nos pedidos!")

    resultado = (
      pedidos_df
          .filter((col("value") > 0) & (col("value").isNotNull()))
          .groupBy("client_id")
          .agg(
              count("*").alias("Qtd_Pedidos"),
              _sum("value").cast(DecimalType(11, 2)).alias("Valor_Total"),
              avg("value").cast(DecimalType(11, 2)).alias("Media"),
              percentile_approx("value", lit(0.5)).cast(DecimalType(11, 2)).alias("Mediana"),
              percentile_approx("value", lit(0.1)).cast(DecimalType(11, 2)).alias("P10"),
              percentile_approx("value", lit(0.9)).cast(DecimalType(11, 2)).alias("P90")
          )
          .join(clients_df, clients_df.id == col("client_id"), how="left")
          .select(
              col("name").alias("Nome_cliente"),
              col("Qtd_Pedidos"),
              col("Valor_Total"),
              col("Media"),
              col("Mediana"),
              col("P10"),
              col("P90")
          )
          .orderBy(col("Valor_Total").desc())
    )

    media_valor_total = resultado.agg(avg("Valor_Total")).collect()[0][0] 
    p10_valor_total = resultado.agg(percentile_approx("Valor_Total", lit(0.1)).cast(DecimalType(11, 2))).collect()[0][0] 
    p90_valor_total = resultado.agg(percentile_approx("Valor_Total", lit(0.9)).cast(DecimalType(11, 2))).collect()[0][0] 
    resultado_sem_outliers = resultado.filter((col("Valor_Total") >= p10_valor_total) & (col("Valor_Total") <= p90_valor_total)
                                              ).orderBy(col("Valor_Total").desc())
    acima_media = resultado.filter(col("Valor_Total") > media_valor_total
                                   ).orderBy(col("Valor_Total").desc())

    print("=" * 60)
    resultado.show(200, truncate=False)
    print("=" * 60)
    print(f"Clientes com valor total acima da media: {acima_media.count()}")
    acima_media.show(200, truncate=False)
    print(f"Clientes com valor total entre P10 e P90: {resultado_sem_outliers.count()}")
    resultado_sem_outliers.show(200, truncate=False)
    print("=" * 60)

    print(f"Total de clientes analisados: {resultado.count()}")
    print(f"Total de pedidos: {pedidos_df.count()}")
    print(f"Valor total geral: {pedidos_df.agg(_sum('value')).collect()[0][0]:.2f}")
except Exception as e:
    print(f"Erro durante a execução: {e}")
finally:
    spark.stop()
    print("\nAnálise concluída.")