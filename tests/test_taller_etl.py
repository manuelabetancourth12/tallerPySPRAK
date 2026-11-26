import csv
import unittest
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

from pyspark.sql import SparkSession

from taller_etl import COLUMNS, run


def read_report(directory, name):
    with (directory / f"{name}.csv").open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


class TestETL(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[2]").appName("Pruebas del ETL")
            .config("spark.sql.shuffle.partitions", "2").getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_pipeline_and_all_reports(self):
        rows = [
            ("1", "A", 'Lámpara, "azul"', "2", "2011-01-01 10:00:00", "10", "10.0", "France"),
            ("1", "A", 'Lámpara, "azul"', "2", "2011-01-01 10:00:00", "10", "10.0", "France"),
            ("2", "B", "Vela", "1", "2/1/2011 10:00", "30", "20", "United Kingdom"),
            ("c3", "A", "Lámpara", "-1", "2011-01-01 11:00:00", "10", "10", "France"),
            ("4", "B", "Vela", "3", "sin fecha", "5", "", "Spain"),
            ("5", "C", "C", "error", "", "9", "30", "Italy"),
            ("6", "C", "C", "2", "", "-9", "30", "Italy"),
        ]
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "input.csv"
            output = root / "out"
            with source.open("w", encoding="utf-8-sig", newline="") as stream:
                writer = csv.writer(stream)
                writer.writerow(COLUMNS)
                writer.writerows(rows)
            self.assertEqual(run(self.spark, source, output), {
                "filas_entrada": 7, "filas_validas": 5, "filas_descartadas": 2,
                "fecha_invalida_o_ausente": 1, "cliente_invalido_o_ausente": 1,
            })
            def report(name):
                return read_report(output, name)

            self.assertEqual(report("01_total_facturas"), [{"facturas": "4"}])
            self.assertEqual(report("02_clientes_unicos"), [{"clientes": "2"}])
            self.assertEqual(report("03_ingreso_total"), [{"ingreso_neto": "75.00"}])
            self.assertEqual(report("04_producto_mas_vendido_top10")[0], {
                "StockCode": "A", "descripcion": 'Lámpara, "azul"', "unidades": "4",
            })
            self.assertEqual(report("05_cliente_mayor_compra_top10")[0]["CustomerID"], "10")
            self.assertEqual(
                [row["Country"] for row in report("06_top5_paises")], ["France", "Spain"]
            )
            self.assertEqual(report("07_ticket_promedio"), [{"ticket": "28.33"}])
            self.assertEqual(report("08_estadisticas_productos_por_factura"), [
                {"minimo": "1", "maximo": "4", "promedio": "2.67"}
            ])
            self.assertEqual(
                [row["Month"] for row in report("09_ventas_por_mes")], ["2011-01", "2011-02"]
            )
            self.assertEqual(report("10_porcentaje_devoluciones"), [
                {"facturas": "4", "con_devolucion": "1", "porcentaje": "25.0"}
            ])
            totals = report("11_clientes_con_ventas")
            self.assertEqual(
                [(row["CustomerID"], Decimal(row["importe_neto"])) for row in totals],
                [("10", Decimal("30")), ("20", Decimal("30"))],
            )
            self.assertEqual(
                [(row["CustomerID"], row["ranking"]) for row in report("12_ranking_clientes")],
                [("10", "1"), ("20", "2")],
            )

    def test_bad_header_fails_before_writing(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "input.csv"
            source.write_text("InvoiceNo,Quantity\n1,2\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "Encabezado inesperado"):
                run(self.spark, source, root / "out")
            self.assertFalse((root / "out").exists())

    def test_header_only_input(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "input.csv"
            source.write_text(",".join(COLUMNS) + "\n", encoding="utf-8")
            output = root / "out"
            self.assertEqual(run(self.spark, source, output)["filas_validas"], 0)
            self.assertEqual(read_report(output, "03_ingreso_total"), [{"ingreso_neto": "0.00"}])
            self.assertEqual(read_report(output, "10_porcentaje_devoluciones"), [
                {"facturas": "0", "con_devolucion": "0", "porcentaje": ""}
            ])
            self.assertEqual(read_report(output, "12_ranking_clientes"), [])


if __name__ == "__main__":
    unittest.main()
