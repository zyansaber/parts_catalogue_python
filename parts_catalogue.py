#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ODBC -> SAP HANA -> Firebase Realtime Database (project: partssr)

- RTDB URL: https://partssr-default-rtdb.asia-southeast1.firebasedatabase.app
- Service account JSON beside this script as: firebase-adminsdk.json
- Writes to: /material_summary_2025/{Material}

Install:
  pip install pyodbc firebase-admin pandas
"""

import os
import math
import logging
from decimal import Decimal
from typing import Any, Dict

import pyodbc
import pandas as pd
import firebase_admin
from firebase_admin import credentials, db

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("firebase_sync.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("SAPtoFirebase")

# ---------- Firebase init (partssr) ----------
FIREBASE_SA = "firebase-adminsdk.json"  # <- partssr 项目的 service account
FIREBASE_DB_URL = "https://partssr-default-rtdb.asia-southeast1.firebasedatabase.app"

if not os.path.exists(FIREBASE_SA):
    raise FileNotFoundError(
        f"Missing Firebase service account file: {FIREBASE_SA}\n"
        "Download from Firebase Console (partssr) > Project settings > Service accounts."
    )

cred = credentials.Certificate(FIREBASE_SA)
firebase_admin.initialize_app(cred, {'databaseURL': FIREBASE_DB_URL})
logger.info("Initialized Firebase Admin SDK (partssr).")

# ---------- SAP HANA ODBC DSN ----------
DSN = (
    "DRIVER={HDBODBC};"
    "SERVERNODE=10.11.2.25:30241;"
    "UID=BAOJIANFENG;"
    "PWD=Xja@2025ABC;"
)

# ---------- SQL ----------
SQL = r"""WITH
/* ===== 1) Standard Price (MBEW, BWKEY=3111) ===== */
std_price AS (
  SELECT
    "MATNR",
    "STPRS" AS "Standard_Price"
  FROM "SAPHANADB"."MBEW"
  WHERE "BWKEY" = '3111'
),

/* ===== 2) Current Stock: WERKS=3111, LGORT in (0001,0019) ===== */
stock AS (
  SELECT
    "MATNR",
    SUM("LABST") AS "Current_Stock_Qty"
  FROM "SAPHANADB"."NSDM_V_MARD"
  WHERE "WERKS" = '3111'
    AND "LGORT" IN ('0001','0019')
  GROUP BY "MATNR"
),

/* ===== 3) Net PO GR 2025-to-date (101/105 + ; 102/106/122 -) ===== */
po_gr AS (
  SELECT
    ekpo."MATNR",
    SUM(
      CASE
        WHEN ekbe."BWART" IN ('101','105') THEN ekbe."MENGE"
        WHEN ekbe."BWART" IN ('102','106','122') THEN -ekbe."MENGE"
        ELSE 0
      END
    ) AS "Purchase_Qty_2025_to_Date"
  FROM "SAPHANADB"."EKKO" ekko
  JOIN "SAPHANADB"."EKPO" ekpo
    ON ekko."EBELN" = ekpo."EBELN"
  JOIN "SAPHANADB"."EKBE" ekbe
    ON ekpo."EBELN" = ekbe."EBELN"
   AND ekpo."EBELP" = ekbe."EBELP"
  WHERE ekko."EKORG" = '3111'
    AND ekpo."WERKS" = '3111'
    AND ekpo."MATNR" IS NOT NULL
    AND LENGTH(TRIM(ekpo."MATNR")) > 0
    AND ekbe."BEWTP" = 'E'
    AND ekbe."BUDAT" >= DATE '2025-01-01'
    AND ekbe."BUDAT" <= CURRENT_DATE
    AND ekbe."BWART" IN ('101','102','105','106','122')
  GROUP BY ekpo."MATNR"
),

/* ===== 4) Recent Vendor by last EKBE.BUDAT ===== */
recent_vendor AS (
  SELECT
    ekpo."MATNR",
    ekko."LIFNR",
    ekbe."BUDAT",
    ROW_NUMBER() OVER (
      PARTITION BY ekpo."MATNR"
      ORDER BY ekbe."BUDAT" DESC
    ) AS rn
  FROM "SAPHANADB"."EKKO" ekko
  JOIN "SAPHANADB"."EKPO" ekpo
    ON ekko."EBELN" = ekpo."EBELN"
  JOIN "SAPHANADB"."EKBE" ekbe
    ON ekpo."EBELN" = ekbe."EBELN"
   AND ekpo."EBELP" = ekbe."EBELP"
  WHERE ekko."EKORG" = '3111'
    AND ekpo."WERKS" = '3111'
    AND ekpo."MATNR" IS NOT NULL
    AND LENGTH(TRIM(ekpo."MATNR")) > 0
    AND ekbe."BEWTP" = 'E'
    AND ekbe."BUDAT" >= DATE '2025-01-01'
    AND ekbe."BUDAT" <= CURRENT_DATE
    AND ekbe."BWART" IN ('101','102','105','106','122')
),
recent_vendor_top AS (
  SELECT
    "MATNR",
    "LIFNR"
  FROM recent_vendor
  WHERE rn = 1
),

/* ===== 5) Pricing Params (Z5 channel, ZP00, valid today) ===== */
par AS (
  SELECT
    '3110'       AS "VKORG",
    'Z5'         AS "VTWEG",
    CURRENT_DATE AS "VALID_ON"
  FROM DUMMY
),
cond_types AS (
  SELECT 'ZP00' AS "KSCHL" FROM DUMMY
),

/* ===== 6) Dealer Price (A004: VKORG/VTWEG/MATNR) ===== */
dealer_price AS (
  SELECT
    a."MATNR",
    CASE
      WHEN CAST(a."DATAB" AS NVARCHAR(20)) LIKE '____-__-__'
        THEN CAST(a."DATAB" AS DATE)
      WHEN CAST(a."DATAB" AS NVARCHAR(20)) LIKE '____.__.__'
        THEN TO_DATE(CAST(a."DATAB" AS NVARCHAR(20)), 'YYYY.MM.DD')
      ELSE TO_DATE(CAST(a."DATAB" AS NVARCHAR(20)), 'YYYYMMDD')
    END AS "Valid_From",
    CASE
      WHEN CAST(a."DATBI" AS NVARCHAR(20)) LIKE '____-__-__'
        THEN CAST(a."DATBI" AS DATE)
      WHEN CAST(a."DATBI" AS NVARCHAR(20)) LIKE '____.__.__'
        THEN TO_DATE(CAST(a."DATBI" AS NVARCHAR(20)), 'YYYY.MM.DD')
      ELSE TO_DATE(CAST(a."DATBI" AS NVARCHAR(20)), 'YYYYMMDD')
    END AS "Valid_To",
    CASE
      WHEN COALESCE(k."KPEIN",0) = 0
        THEN CAST(k."KBETR" AS DECIMAL(31,6))
      ELSE CAST(k."KBETR" AS DECIMAL(31,6)) / k."KPEIN"
    END AS "Dealer_Price",
    k."KONWA" AS "Dealer_Currency",
    k."KMEIN" AS "Dealer_Unit"
  FROM "SAPHANADB"."A004" a
  JOIN "SAPHANADB"."KONH" h
    ON h."KNUMH" = a."KNUMH"
  JOIN "SAPHANADB"."KONP" k
    ON k."KNUMH" = h."KNUMH"
  JOIN cond_types ct
    ON ct."KSCHL" = h."KSCHL"
  JOIN par p
    ON p."VKORG" = a."VKORG"
   AND p."VTWEG" = a."VTWEG"
  WHERE
    (CASE
       WHEN CAST(a."DATAB" AS NVARCHAR(20)) LIKE '____-__-__'
         THEN CAST(a."DATAB" AS DATE)
       WHEN CAST(a."DATAB" AS NVARCHAR(20)) LIKE '____.__.__'
         THEN TO_DATE(CAST(a."DATAB" AS NVARCHAR(20)), 'YYYY.MM.DD')
       ELSE TO_DATE(CAST(a."DATAB" AS NVARCHAR(20)), 'YYYYMMDD')
     END) <= p."VALID_ON"
    AND
    (CASE
       WHEN CAST(a."DATBI" AS NVARCHAR(20)) LIKE '____-__-__'
         THEN CAST(a."DATBI" AS DATE)
       WHEN CAST(a."DATBI" AS NVARCHAR(20)) LIKE '____.__.__'
         THEN TO_DATE(CAST(a."DATBI" AS NVARCHAR(20)), 'YYYY.MM.DD')
       ELSE TO_DATE(CAST(a."DATBI" AS NVARCHAR(20)), 'YYYYMMDD')
     END) >= p."VALID_ON"
),

/* ===== 7) Customer Price (A032: KUNNR/PR00GRP + MATNR) ===== */
customer_price AS (
  SELECT
    a."MATNR",
    CASE
      WHEN CAST(a."DATAB" AS NVARCHAR(20)) LIKE '____-__-__'
        THEN CAST(a."DATAB" AS DATE)
      WHEN CAST(a."DATAB" AS NVARCHAR(20)) LIKE '____.__.__'
        THEN TO_DATE(CAST(a."DATAB" AS NVARCHAR(20)), 'YYYY.MM.DD')
      ELSE TO_DATE(CAST(a."DATAB" AS NVARCHAR(20)), 'YYYYMMDD')
    END AS "Valid_From",
    CASE
      WHEN CAST(a."DATBI" AS NVARCHAR(20)) LIKE '____-__-__'
        THEN CAST(a."DATBI" AS DATE)
      WHEN CAST(a."DATBI" AS NVARCHAR(20)) LIKE '____.__.__'
        THEN TO_DATE(CAST(a."DATBI" AS NVARCHAR(20)), 'YYYY.MM.DD')
      ELSE TO_DATE(CAST(a."DATBI" AS NVARCHAR(20)), 'YYYYMMDD')
    END AS "Valid_To",
    CASE
      WHEN COALESCE(k."KPEIN",0) = 0
        THEN CAST(k."KBETR" AS DECIMAL(31,6))
      ELSE CAST(k."KBETR" AS DECIMAL(31,6)) / k."KPEIN"
    END AS "Customer_Price",
    k."KONWA" AS "Customer_Currency",
    k."KMEIN" AS "Customer_Unit"
  FROM "SAPHANADB"."A032" a
  JOIN "SAPHANADB"."KONH" h
    ON h."KNUMH" = a."KNUMH"
  JOIN "SAPHANADB"."KONP" k
    ON k."KNUMH" = h."KNUMH"
  JOIN cond_types ct
    ON ct."KSCHL" = h."KSCHL"
  JOIN par p
    ON p."VKORG" = a."VKORG"
   AND p."VTWEG" = a."VTWEG"
  WHERE
    (CASE
       WHEN CAST(a."DATAB" AS NVARCHAR(20)) LIKE '____-__-__'
         THEN CAST(a."DATAB" AS DATE)
       WHEN CAST(a."DATAB" AS NVARCHAR(20)) LIKE '____.__.__'
         THEN TO_DATE(CAST(a."DATAB" AS NVARCHAR(20)), 'YYYY.MM.DD')
       ELSE TO_DATE(CAST(a."DATAB" AS NVARCHAR(20)), 'YYYYMMDD')
     END) <= p."VALID_ON"
    AND
    (CASE
       WHEN CAST(a."DATBI" AS NVARCHAR(20)) LIKE '____-__-__'
         THEN CAST(a."DATBI" AS DATE)
       WHEN CAST(a."DATBI" AS NVARCHAR(20)) LIKE '____.__.__'
         THEN TO_DATE(CAST(a."DATBI" AS NVARCHAR(20)), 'YYYY.MM.DD')
       ELSE TO_DATE(CAST(a."DATBI" AS NVARCHAR(20)), 'YYYYMMDD')
     END) >= p."VALID_ON"
),

/* ===== 8) SO Qty 2025 (VKORG=3110, WERKS=3111) ===== */
so_qty_2025 AS (
  SELECT
    vbap."MATNR",
    SUM(vbap."KWMENG") AS "Sales_Qty_SO_2025"
  FROM "SAPHANADB"."VBAK" vbak
  JOIN "SAPHANADB"."VBAP" vbap
    ON vbak."VBELN" = vbap."VBELN"
  WHERE vbak."VKORG" = '3110'
    AND vbap."WERKS" = '3111'
    AND vbak."AUDAT" >= DATE '2025-01-01'
    AND vbak."AUDAT" <= CURRENT_DATE
    AND vbap."MATNR" IS NOT NULL
    AND LENGTH(TRIM(vbap."MATNR")) > 0
  GROUP BY vbap."MATNR"
),

/* ===== 9) PGI Qty 2025 (MSEG 601+/602-) ===== */
pgi_qty_2025 AS (
  SELECT
    mseg."MATNR",
    SUM(
      CASE
        WHEN mseg."BWART" = '601' THEN mseg."MENGE"
        WHEN mseg."BWART" = '602' THEN -mseg."MENGE"
        ELSE 0
      END
    ) AS "Sales_Qty_PGI_2025"
  FROM "SAPHANADB"."NSDM_V_MSEG" mseg
  WHERE mseg."WERKS" = '3111'
    AND mseg."BUDAT_MKPF" >= DATE '2025-01-01'
    AND mseg."BUDAT_MKPF" <= CURRENT_DATE
    AND mseg."BWART" IN ('601','602')
    AND mseg."MATNR" IS NOT NULL
    AND LENGTH(TRIM(mseg."MATNR")) > 0
  GROUP BY mseg."MATNR"
),

/* ===== 10) Invoice Amount 2025 (VBRP/VBRK, VKORG=3110) ===== */
invoice_amt_2025 AS (
  SELECT
    vbrp."MATNR",
    SUM(CAST(vbrp."NETWR" AS DECIMAL(31,6))) AS "Invoice_Amount_2025",
    CASE
      WHEN COUNT(DISTINCT vbrk."WAERK") = 1
        THEN MIN(vbrk."WAERK")
      ELSE NULL
    END AS "Invoice_Currency"
  FROM "SAPHANADB"."VBRP" vbrp
  JOIN "SAPHANADB"."VBRK" vbrk
    ON vbrp."VBELN" = vbrk."VBELN"
  WHERE vbrk."VKORG" = '3110'
    AND vbrk."FKDAT" >= DATE '2025-01-01'
    AND vbrk."FKDAT" <= CURRENT_DATE
    AND vbrp."MATNR" IS NOT NULL
    AND LENGTH(TRIM(vbrp."MATNR")) > 0
  GROUP BY vbrp."MATNR"
),

/* ===== 11) Merge PO/Stock/Std Price ===== */
base AS (
  SELECT
    COALESCE(p."MATNR", s."MATNR", sp."MATNR") AS "MATNR",
    COALESCE(p."Purchase_Qty_2025_to_Date", 0) AS "Purchase_Qty_2025_to_Date",
    COALESCE(s."Current_Stock_Qty", 0)         AS "Current_Stock_Qty",
    COALESCE(sp."Standard_Price", 0)           AS "Standard_Price"
  FROM po_gr p
  FULL OUTER JOIN stock s
    ON s."MATNR" = p."MATNR"
  FULL OUTER JOIN std_price sp
    ON sp."MATNR" = COALESCE(p."MATNR", s."MATNR")
),

/* ===== 12) Base Filter: non-empty, not D%/TR%, and (PO≠0 or Stock≠0) ===== */
base_filtered AS (
  SELECT *
  FROM base
  WHERE "MATNR" IS NOT NULL
    AND LENGTH(TRIM("MATNR")) > 0
    AND UPPER("MATNR") NOT LIKE 'D%'
    AND UPPER("MATNR") NOT LIKE 'TR%'
    AND (
      COALESCE("Purchase_Qty_2025_to_Date", 0) <> 0
      OR COALESCE("Current_Stock_Qty", 0) <> 0
    )
),

/* ===== 13) New Materials since 2025-01-01 (MARC+MARA, WERKS=3111) ===== */
new_plant AS (
  SELECT
    marc."MATNR"
  FROM "SAPHANADB"."MARC" marc
  JOIN "SAPHANADB"."MARA" mara
    ON mara."MATNR" = marc."MATNR"
  WHERE marc."WERKS" = '3111'
    AND marc."MATNR" IS NOT NULL
    AND LENGTH(TRIM(marc."MATNR")) > 0
    AND UPPER(marc."MATNR") NOT LIKE 'D%'
    AND UPPER(marc."MATNR") NOT LIKE 'TR%'
    AND (
      CASE
        WHEN CAST(mara."ERSDA" AS NVARCHAR(20)) LIKE '____-__-__'
          THEN CAST(mara."ERSDA" AS DATE)
        ELSE TO_DATE(CAST(mara."ERSDA" AS NVARCHAR(20)), 'YYYYMMDD')
      END
    ) >= DATE '2025-01-01'
),

/* ===== 14) Modified since 2025-01-01 (ONLY MARA.LAEDA; your MARC doesn't have LAEDA) ===== */
modified_plant AS (
  SELECT
    marc."MATNR"
  FROM "SAPHANADB"."MARC" marc
  JOIN "SAPHANADB"."MARA" mara
    ON mara."MATNR" = marc."MATNR"
  WHERE marc."WERKS" = '3111'
    AND marc."MATNR" IS NOT NULL
    AND LENGTH(TRIM(marc."MATNR")) > 0
    AND UPPER(marc."MATNR") NOT LIKE 'D%'
    AND UPPER(marc."MATNR") NOT LIKE 'TR%'
    AND (
      CASE
        WHEN mara."LAEDA" IS NULL THEN NULL
        WHEN CAST(mara."LAEDA" AS NVARCHAR(20)) LIKE '____-__-__'
          THEN CAST(mara."LAEDA" AS DATE)
        WHEN CAST(mara."LAEDA" AS NVARCHAR(20)) LIKE '____.__.__'
          THEN TO_DATE(CAST(mara."LAEDA" AS NVARCHAR(20)), 'YYYY.MM.DD')
        ELSE TO_DATE(CAST(mara."LAEDA" AS NVARCHAR(20)), 'YYYYMMDD')
      END
    ) >= DATE '2025-01-01'
),

/* ===== 15) Final Base (3 criteria): base_filtered UNION new_plant UNION modified_plant ===== */
final_base_1 AS (
  SELECT *
  FROM base_filtered

  UNION ALL

  SELECT
    np."MATNR",
    0 AS "Purchase_Qty_2025_to_Date",
    0 AS "Current_Stock_Qty",
    COALESCE(sp."Standard_Price", 0) AS "Standard_Price"
  FROM new_plant np
  LEFT JOIN base_filtered bf
    ON bf."MATNR" = np."MATNR"
  LEFT JOIN std_price sp
    ON sp."MATNR" = np."MATNR"
  WHERE bf."MATNR" IS NULL
),

final_base AS (
  SELECT *
  FROM final_base_1

  UNION ALL

  SELECT
    mp."MATNR",
    0 AS "Purchase_Qty_2025_to_Date",
    0 AS "Current_Stock_Qty",
    COALESCE(sp."Standard_Price", 0) AS "Standard_Price"
  FROM modified_plant mp
  LEFT JOIN final_base_1 fb
    ON fb."MATNR" = mp."MATNR"
  LEFT JOIN std_price sp
    ON sp."MATNR" = mp."MATNR"
  WHERE fb."MATNR" IS NULL
)

SELECT
  f."MATNR"                              AS "Material",
  f."Purchase_Qty_2025_to_Date",
  f."Current_Stock_Qty",
  f."Standard_Price",

  /* ===== Sales metrics ===== */
  COALESCE(so."Sales_Qty_SO_2025",  0)   AS "Sales_Qty_SO_2025",
  COALESCE(pgi."Sales_Qty_PGI_2025", 0)  AS "Sales_Qty_PGI_2025",
  COALESCE(inv."Invoice_Amount_2025", 0) AS "Invoice_Amount_2025",
  inv."Invoice_Currency",

  /* ===== Pricing (Dealer/Customer) ===== */
  dp."Dealer_Price",
  cp."Customer_Price",
  COALESCE(dp."Dealer_Currency", cp."Customer_Currency") AS "Sales_Currency",
  COALESCE(dp."Dealer_Unit",     cp."Customer_Unit")     AS "Sales_Unit",

  /* ===== Texts & Vendor ===== */
  makt."MAKTX"                         AS "SPRAS_EN",
  lfa1."NAME1"                         AS "Supplier_Name",

  /* ===== Price vs Standard (百分比数值) ===== */
  CASE
    WHEN f."Standard_Price" > 0 AND dp."Dealer_Price" IS NOT NULL
      THEN ROUND( (dp."Dealer_Price"   - f."Standard_Price") / f."Standard_Price" * 100, 2 )
    ELSE NULL
  END AS "Dealer_vs_Std_%",
  CASE
    WHEN f."Standard_Price" > 0 AND cp."Customer_Price" IS NOT NULL
      THEN ROUND( (cp."Customer_Price" - f."Standard_Price") / f."Standard_Price" * 100, 2 )
    ELSE NULL
  END AS "Customer_vs_Std_%"

FROM final_base f
LEFT JOIN dealer_price     dp   ON dp."MATNR"  = f."MATNR"
LEFT JOIN customer_price   cp   ON cp."MATNR"  = f."MATNR"
LEFT JOIN so_qty_2025      so   ON so."MATNR"  = f."MATNR"
LEFT JOIN pgi_qty_2025     pgi  ON pgi."MATNR" = f."MATNR"
LEFT JOIN invoice_amt_2025 inv  ON inv."MATNR" = f."MATNR"
LEFT JOIN "SAPHANADB"."MAKT" makt
  ON makt."MATNR" = f."MATNR"
 AND makt."SPRAS" = 'E'
LEFT JOIN recent_vendor_top rv
  ON rv."MATNR" = f."MATNR"
LEFT JOIN "SAPHANADB"."LFA1" lfa1
  ON lfa1."LIFNR" = rv."LIFNR"
LEFT JOIN "SAPHANADB"."MARA" mara
  ON mara."MATNR" = f."MATNR"
WHERE COALESCE(mara."MTART", '')      <> 'DIEN'
  AND COALESCE(mara."MTPOS_MARA", '') <> 'LEIS'
ORDER BY "Material";


"""

# 写入路径 & 批大小
BASE_PATH = "material_summary_2025"   # 如需沿用旧路径，改成 "bom_summary"
BATCH = 500

# 需要写入的列（按 SQL 输出列名）
NUMERIC_COLS = [
    "Purchase_Qty_2025_to_Date",
    "Current_Stock_Qty",
    "Standard_Price",
    "Sales_Qty_SO_2025",
    "Sales_Qty_PGI_2025",
    "Invoice_Amount_2025",
    "Dealer_Price",
    "Customer_Price",
    "Dealer_vs_Std_%",
    "Customer_vs_Std_%"
]
TEXT_COLS = [
    "Invoice_Currency",
    "Sales_Currency",
    "Sales_Unit",
    "SPRAS_EN",
    "Supplier_Name"
]

def to_json_number(v: Any) -> Any:
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    if isinstance(v, Decimal):
        v = float(v)
    if hasattr(v, "item") and callable(getattr(v, "item")):
        try:
            v = v.item()
        except Exception:
            pass
    if isinstance(v, (int, float)):
        return float(v) if math.isfinite(float(v)) else None
    return None

def to_json_text(v: Any) -> Any:
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    s = str(v).strip()
    return s if s else None

def row_to_payload(row: pd.Series) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    for col in NUMERIC_COLS:
        if col in row:
            val = to_json_number(row[col])
            if val is not None:
                payload[col] = val
    for col in TEXT_COLS:
        if col in row:
            val = to_json_text(row[col])
            if val is not None:
                payload[col] = val
    return payload

def df_to_updates(df: pd.DataFrame) -> list[Dict[str, Dict[str, Any]]]:
    updates = []
    chunk: Dict[str, Dict[str, Any]] = {}
    if "Material" not in df.columns:
        raise KeyError("SQL 结果缺少必需列：'Material'")
    for _, row in df.iterrows():
        material = str(row.get("Material") or "").strip()
        if not material:
            continue
        item = row_to_payload(row)
        if not item:
            continue
        chunk[material] = item
        if len(chunk) >= BATCH:
            updates.append(chunk)
            chunk = {}
    if chunk:
        updates.append(chunk)
    return updates

def main():
    logger.info("Connecting to SAP HANA via ODBC...")
    pyodbc.pooling = False
    with pyodbc.connect(DSN, autocommit=True) as conn:
        logger.info("Running SQL...")
        df = pd.read_sql(SQL, conn)

    logger.info("Fetched %d rows.", len(df))
    if not df.empty:
        show_cols = [c for c in ["Material", "SPRAS_EN", "Supplier_Name",
                                 "Current_Stock_Qty", "Sales_Qty_PGI_2025",
                                 "Standard_Price", "Dealer_Price", "Customer_Price",
                                 "Dealer_vs_Std_%", "Customer_vs_Std_%"]
                     if c in df.columns]
        logger.info("Preview:\n%s", df[show_cols].head(5).to_string(index=False))

    logger.info("Preparing updates...")
    updates = df_to_updates(df)
    logger.info("Prepared %d chunk(s).", len(updates))

    ref = db.reference(BASE_PATH)

    # ======== 关键新增：先删旧数据，再上传 ========
    logger.warning("Deleting existing data under '/%s' ...", BASE_PATH)
    ref.delete()
    logger.info("Path cleared. Start uploading fresh data...")

    # 批量上传
    for i, payload in enumerate(updates, 1):
        ref.update(payload)
        logger.info("Uploaded chunk %d/%d (%d records)", i, len(updates), len(payload))

    logger.info("Done.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception("FAILED: %s", e)
        raise
