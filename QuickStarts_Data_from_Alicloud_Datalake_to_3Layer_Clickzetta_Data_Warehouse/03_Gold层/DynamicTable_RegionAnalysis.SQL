CREATE OR       
REPLACE DYNAMIC TABLE `three_layer_dwh_sch`.`dt_region_analysis` (
`REGION`,
`STORE_TYPE`,
`TOTAL_SALES`,
`AVG_SALE`,
`AVG_QUANTITY`
)
REFRESH   INTERVAL 1 MINUTE VCLUSTER THREE_LAYER_DWH_VC AS (
SELECT    CASE
                    WHEN c.COUNTRY IN ('USA', 'Canada') THEN 'NA'
                    WHEN c.COUNTRY IN ('Brazil') THEN 'SA'
                    WHEN c.COUNTRY IN ('Australia') THEN 'AUS'
                    WHEN c.COUNTRY IN ('Germany', 'UK', 'France') THEN 'EU'
                    WHEN c.COUNTRY IN ('China', 'India', 'Japan') THEN 'ASIA'
                    ELSE 'UNKNOWN'
          END AS REGION,
          o.STORE_TYPE,
          SUM(o.TOTAL_AMOUNT) AS TOTAL_SALES,
          AVG(o.TOTAL_AMOUNT) AS AVG_SALE,
          AVG(o.QUANTITY) AS AVG_QUANTITY
FROM      SILVER_ORDERS AS o
JOIN      SILVER_PRODUCT AS p ON o.product_id = p.product_id
JOIN      SILVER_CUSTOMER AS c ON o.customer_id = c.customer_id
GROUP BY  REGION,
          o.STORE_TYPE
ORDER BY  TOTAL_SALES DESC,
          AVG_SALE DESC,
          AVG_QUANTITY DESC
);