"""Taller ETL de Online Retail con PySpark."""
import argparse
import csv
import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile

from pyspark.sql import SparkSession, Window, functions as F

ROOT = Path(__file__).resolve().parent
COLUMNS = (
    "InvoiceNo", "StockCode", "Description", "Quantity",
    "InvoiceDate", "UnitPrice", "CustomerID", "Country",
)


def read_transactions(spark, source):
    """Verifica el encabezado antes de convertir los datos."""
    with source.open(encoding="utf-8-sig", newline="") as stream:
        header = next(csv.reader(stream), [])
    if tuple(header) != COLUMNS:
        raise ValueError(f"Encabezado inesperado en {source}: {header}")
    return spark.read.option("header", True).option("mode", "FAILFAST").option(
        "escape", chr(34)
    ).option("multiLine", True).csv(
        source.resolve().as_uri()
    )


def transform(raw):
    """Normaliza los campos y separa ventas de devoluciones."""
    clean = (
        raw.select(*(F.trim(F.col(name)).alias(name) for name in COLUMNS))
        .withColumn("InvoiceNo", F.upper("InvoiceNo"))
        .withColumn("Quantity", F.expr("try_cast(Quantity AS BIGINT)"))
        .withColumn("UnitPrice", F.expr("try_cast(UnitPrice AS DECIMAL(16,4))"))
        .withColumn(
            "InvoiceDate",
            F.coalesce(
                F.try_to_timestamp("InvoiceDate", F.lit("yyyy-MM-dd HH:mm:ss")),
                F.try_to_timestamp("InvoiceDate", F.lit("M/d/yyyy H:mm")),
            ),
        )
        .withColumn(
            "CustomerID",
            F.when(
                F.col("CustomerID").rlike(r"^[0-9]+(?:[.]0+)?$"),
                F.regexp_replace("CustomerID", r"[.]0+$", ""),
            ),
        )
        .withColumn("Revenue", F.col("Quantity") * F.col("UnitPrice"))
        .withColumn("Month", F.date_format("InvoiceDate", "yyyy-MM"))
        .withColumn(
            "IsReturn",
            F.col("InvoiceNo").startswith("C") | (F.col("Quantity") < 0),
        )
    )
    return clean.filter(
        (F.length("InvoiceNo") > 0)
        & F.col("Quantity").isNotNull()
        & (F.col("UnitPrice") >= 0)
    )


def reports(transactions):
    """Devuelve las diez respuestas y los dos análisis complementarios."""
    sales = transactions.filter(~F.col("IsReturn") & (F.col("Quantity") > 0))
    identified = transactions.filter(F.col("CustomerID").isNotNull())
    invoices = transactions.groupBy("InvoiceNo").agg(
        F.max(F.col("IsReturn").cast("int")).alias("has_return")
    )
    returns = invoices.agg(
        F.count("*").alias("facturas"),
        F.coalesce(F.sum("has_return"), F.lit(0)).alias("con_devolucion"),
    ).withColumn(
        "porcentaje", F.round(F.try_divide(F.col("con_devolucion") * 100, F.col("facturas")), 2)
    )
    sales_by_invoice = sales.groupBy("InvoiceNo").agg(
        F.sum("Revenue").alias("total"), F.sum("Quantity").alias("unidades")
    )
    products = (
        sales.filter(F.length("StockCode") > 0)
        .groupBy("StockCode")
        .agg(F.min("Description").alias("descripcion"), F.sum("Quantity").alias("unidades"))
        .orderBy(F.desc("unidades"), "StockCode")
    )
    customer_totals = identified.groupBy("CustomerID").agg(
        F.sum("Revenue").alias("importe_neto")
    )
    countries = (
        sales.filter((F.length("Country") > 0) & (F.col("Country") != "United Kingdom"))
        .groupBy("Country")
        .agg(F.sum("Revenue").alias("ventas"))
        .orderBy(F.desc("ventas"), "Country")
    )
    monthly = (
        sales.filter(F.col("Month").isNotNull())
        .groupBy("Month")
        .agg(F.sum("Revenue").alias("ventas"))
        .orderBy(F.desc("ventas"), "Month")
    )
    customers = identified.groupBy("CustomerID").agg(
        F.countDistinct("Country").alias("paises")
    )
    joined = customers.join(customer_totals, on="CustomerID", how="inner")
    rank_window = Window.orderBy(F.desc("importe_neto"), "CustomerID")

    return {
        "01_total_facturas": invoices.agg(F.count("*").alias("facturas")),
        "02_clientes_unicos": identified.agg(F.countDistinct("CustomerID").alias("clientes")),
        "03_ingreso_total": transactions.agg(
            F.round(F.coalesce(F.sum("Revenue"), F.lit(0)), 2).alias("ingreso_neto")
        ),
        "04_producto_mas_vendido_top10": products.limit(10),
        "05_cliente_mayor_compra_top10": customer_totals.orderBy(
            F.desc("importe_neto"), "CustomerID"
        ).limit(10),
        "06_top5_paises": countries.limit(5),
        "07_ticket_promedio": sales_by_invoice.agg(F.round(F.avg("total"), 2).alias("ticket")),
        "08_estadisticas_productos_por_factura": sales_by_invoice.agg(
            F.min("unidades").alias("minimo"),
            F.max("unidades").alias("maximo"),
            F.round(F.avg("unidades"), 2).alias("promedio"),
        ),
        "09_ventas_por_mes": monthly,
        "10_porcentaje_devoluciones": returns,
        "11_clientes_con_ventas": joined.orderBy("CustomerID"),
        "12_ranking_clientes": joined.withColumn(
            "ranking", F.row_number().over(rank_window)
        ).orderBy("ranking"),
    }


def export_csv(frame, destination):
    """Escribe un CSV local íntegro, incluso si falla la exportación."""
    temporary = None
    try:
        with NamedTemporaryFile(
            mode="w", encoding="utf-8-sig", newline="", dir=destination.parent,
            delete=False,
        ) as stream:
            temporary = Path(stream.name)
            writer = csv.writer(stream)
            writer.writerow(frame.columns)
            writer.writerows(frame.toLocalIterator())
        os.replace(temporary, destination)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def run(spark, source, output_dir):
    raw = read_transactions(spark, source)
    transactions = transform(raw).cache()
    try:
        quality = {
            "filas_entrada": raw.count(),
            "filas_validas": transactions.count(),
            "fecha_invalida_o_ausente": transactions.filter(F.col("InvoiceDate").isNull()).count(),
            "cliente_invalido_o_ausente": transactions.filter(F.col("CustomerID").isNull()).count(),
        }
        quality["filas_descartadas"] = quality["filas_entrada"] - quality["filas_validas"]
        output_dir.mkdir(parents=True, exist_ok=True)
        for name, frame in reports(transactions).items():
            export_csv(frame, output_dir / f"{name}.csv")
        (output_dir / "calidad.json").write_text(
            json.dumps(quality, indent=2) + "\n", encoding="utf-8"
        )
        return quality
    finally:
        transactions.unpersist()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-path", type=Path, default=ROOT / "data" / "online_retail.csv")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "resultados")
    parser.add_argument("--master", default="local[2]")
    args = parser.parse_args()
    spark = (
        SparkSession.builder.appName("Taller Online Retail")
        .master(args.master)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        print(json.dumps(run(spark, args.data_path, args.output_dir), indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
