import csv
import json
import re
from collections import Counter, defaultdict
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def rows(path):
    with path.open(encoding="utf-8-sig", newline="") as stream:
        yield from csv.DictReader(stream)


def rounded(value):
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def report(pattern):
    return list(rows(next((ROOT / "resultados").glob(pattern + "*.csv"))))


def compare(label, actual, expected):
    if actual != expected:
        raise AssertionError(f"{label}: esperado {str(expected)[:250]}, obtenido {str(actual)[:250]}")


def main():
    invoices, returns = set(), set()
    customers = defaultdict(Decimal)
    customer_countries = defaultdict(set)
    sales_amount, sales_units = defaultdict(Decimal), Counter()
    products, countries, months = Counter(), defaultdict(Decimal), defaultdict(Decimal)
    descriptions = defaultdict(set)
    quality = Counter({
        "filas_entrada": 0, "filas_validas": 0, "filas_descartadas": 0,
        "fecha_invalida_o_ausente": 0, "cliente_invalido_o_ausente": 0,
    })
    revenue = Decimal(0)
    for row in rows(ROOT / "data" / "online_retail.csv"):
        quality["filas_entrada"] += 1
        invoice = row["InvoiceNo"].strip().upper()
        units = int(row["Quantity"])
        price = Decimal(row["UnitPrice"])
        if not invoice or price < 0:
            quality["filas_descartadas"] += 1
            continue
        quality["filas_validas"] += 1
        invoices.add(invoice)
        value = units * price
        revenue += value
        is_return = invoice.startswith("C") or units < 0
        if is_return:
            returns.add(invoice)
        customer = row["CustomerID"].strip()
        country = row["Country"].strip()
        if re.fullmatch(r"[0-9]+(?:[.]0+)?", customer):
            customer = customer.split(".")[0]
            customers[customer] += value
            if country:
                customer_countries[customer].add(country)
        else:
            quality["cliente_invalido_o_ausente"] += 1
        month = None
        try:
            month = datetime.fromisoformat(row["InvoiceDate"]).strftime("%Y-%m")
        except ValueError:
            quality["fecha_invalida_o_ausente"] += 1
        if not is_return and units > 0:
            sales_amount[invoice] += value
            sales_units[invoice] += units
            stock = row["StockCode"].strip()
            if stock:
                products[stock] += units
                if row["Description"]:
                    descriptions[stock].add(row["Description"].strip())
            if country and country != "United Kingdom":
                countries[country] += value
            if month:
                months[month] += value

    def ranked(totals):
        return sorted(totals.items(), key=lambda item: (-item[1], item[0]))

    compare("calidad", json.loads((ROOT / "resultados" / "calidad.json").read_text()), quality)
    compare("01", int(report("01")[0]["facturas"]), len(invoices))
    compare("02", int(report("02")[0]["clientes"]), len(customers))
    compare("03", Decimal(report("03")[0]["ingreso_neto"]), rounded(revenue))
    compare("04", [(r["StockCode"], int(r["unidades"]), r["descripcion"]) for r in report("04")],
            [(code, units, min(descriptions[code], default="")) for code, units in ranked(products)[:10]])
    compare("05", [(r["CustomerID"], Decimal(r["importe_neto"])) for r in report("05")],
            ranked(customers)[:10])
    compare("06", [(r["Country"], Decimal(r["ventas"])) for r in report("06")], ranked(countries)[:5])
    compare("07", Decimal(report("07")[0]["ticket"]), rounded(sum(sales_amount.values()) / len(sales_amount)))
    stats = report("08")[0]
    compare("08", (int(stats["minimo"]), int(stats["maximo"]), Decimal(stats["promedio"])), (
        min(sales_units.values()), max(sales_units.values()),
        rounded(Decimal(sum(sales_units.values())) / len(sales_units)),
    ))
    compare("09", [(r["Month"], Decimal(r["ventas"])) for r in report("09")], ranked(months))
    refunds = report("10")[0]
    compare("10", (int(refunds["facturas"]), int(refunds["con_devolucion"]), Decimal(refunds["porcentaje"])),
            (len(invoices), len(returns), rounded(Decimal(100 * len(returns)) / len(invoices))))
    compare("11", [(r["CustomerID"], Decimal(r["importe_neto"]), int(r["paises"])) for r in report("11")],
            [(customer, customers[customer], len(customer_countries[customer])) for customer in sorted(customers)])
    compare("12", [(r["CustomerID"], Decimal(r["importe_neto"]), int(r["ranking"])) for r in report("12")],
            [(customer, total, rank) for rank, (customer, total) in enumerate(ranked(customers), 1)])
    print(f"Conciliación correcta: 12 CSV y calidad, {quality['filas_entrada']:,} filas de entrada.")


if __name__ == "__main__":
    main()
