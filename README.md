# Taller ETL con PySpark — Online Retail

El proyecto procesa las transacciones del CSV suministrado con el taller. Genera diez respuestas, dos análisis complementarios y un informe de calidad.

## Ejecutar desde la raíz

Entorno de referencia probado con Python 3.14, Java 17 y PySpark 4.2.0. Configura `JAVA_HOME` si el comando `java -version` selecciona una versión anterior de Java.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe taller_etl.py
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
.\.venv\Scripts\python.exe tests/verificar_dataset.py
```

El ETL lee `data/online_retail.csv` y escribe en `resultados/`. Puedes especificar otro origen y destino con `--data-path` y `--output-dir`. El verificador independiente usa el CSV suministrado y el destino predeterminado.

## Criterios de cálculo

- Los conteos de facturas incluyen cada identificador válido una vez, incluso cuando corresponde a una devolución. Se identifica una devolución por prefijo `C` o cantidad negativa.
- El ingreso neto suma cantidad firmada por precio unitario. El importe neto por cliente usa el mismo criterio. El ticket, las unidades por factura, los países y los meses se calculan con ventas de cantidad positiva, excluyendo devoluciones.
- El producto se identifica por `StockCode`. Se conserva una descripción por código y se ordenan los productos por unidades vendidas. Los meses se agrupan por año y mes.
- Se conservan las líneas repetidas porque el CSV carece de un identificador de línea que permita distinguir un duplicado de una compra repetida. Se descartan las líneas con factura ausente, cantidad inválida o precio inválido o negativo. Un cliente ausente queda fuera de las métricas por cliente y una fecha inválida queda fuera de las métricas mensuales.
- El `join` combina la cantidad de países distintos con el importe neto por cliente. La ventana asigna posiciones consecutivas por importe descendente y resuelve empates por identificador de cliente.

## Salidas

| CSV | Pregunta |
| --- | --- |
| 01 | Total de facturas |
| 02 | Clientes únicos |
| 03 | Ingreso neto |
| 04 | Productos más vendidos, top 10 |
| 05 | Clientes con mayor compra neta, top 10 |
| 06 | Países con más ventas fuera de Reino Unido, top 5 |
| 07 | Ticket promedio por factura de venta |
| 08 | Mínimo, máximo y promedio de unidades por factura de venta |
| 09 | Ventas por año y mes, ordenadas del mayor al menor importe |
| 10 | Facturas con devoluciones y porcentaje |
| 11 | Clientes con ventas y cantidad de países, mediante un `join` |
| 12 | Ranking de clientes, mediante una función de ventana |

`calidad.json` registra los recuentos de entrada, filas válidas, descartes y ausencias. Las pruebas cubren las doce salidas, devoluciones, ventas repetidas, CSV con comillas, empates, fechas inválidas, encabezado incorrecto y un CSV vacío con encabezado. El verificador del dataset calcula las mismas métricas con la biblioteca estándar de Python y compara los archivos exportados.

## Resultados del CSV suministrado

| Pregunta | Respuesta |
| --- | --- |
| Facturas | 25.898 |
| Clientes únicos | 4.372 |
| Ingreso neto | £9.769.872,05 |
| Producto más vendido | Código 23843, Paper Craft, Little Birdie, con 80.995 unidades |
| Cliente con mayor compra neta | Cliente 14646, con £279.489,02 |
| Países fuera de Reino Unido | Países Bajos, Irlanda (EIRE), Alemania, Francia y Australia |
| Ticket promedio de venta | £514,65 |
| Unidades por factura de venta | Mínimo 1, máximo 80.995 y promedio 273,13 |
| Mes de mayores ventas | Noviembre de 2011, con £1.509.496,33 |
| Facturas con devoluciones | 5.172 de 25.898, equivalentes al 19,97 % |

De 541.909 filas procesadas, 541.907 pasaron la validación. Entre las filas válidas, 135.078 carecían de un cliente identificable. Estas filas sí aportaron a las métricas que se calculan por factura o producto.
