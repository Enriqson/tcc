SELECT
    D_YEAR,
    S_CITY,
    P_BRAND,
    sum(LO_REVENUE - LO_SUPPLYCOST) AS profit
FROM
    {date_table},
    {customer_table},
    {supplier_table},
    {part_table},
    {lineorder_table}
WHERE
    LO_CUSTKEY = C_CUSTKEY
    AND LO_SUPPKEY = S_SUPPKEY
    AND LO_PARTKEY = P_PARTKEY
    AND LO_ORDERDATE = D_DATEKEY
    AND C_REGION = 'AMERICA'
    AND S_NATION = 'UNITED STATES'
    AND (D_YEAR = 1997 OR D_YEAR = 1998)
    AND P_CATEGORY = 'MFGR#14'
GROUP BY
    D_YEAR,
    S_CITY,
    P_BRAND
ORDER BY
    D_YEAR,
    S_CITY,
    P_BRAND