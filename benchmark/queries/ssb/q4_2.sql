SELECT
    D_YEAR,
    S_NATION,
    P_CATEGORY,
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
    AND S_REGION = 'AMERICA'
    AND (D_YEAR = 1997 OR D_YEAR = 1998)
    AND (P_MFGR = 'MFGR#1' OR P_MFGR = 'MFGR#2')
GROUP BY
    D_YEAR,
    S_NATION,
    P_CATEGORY
ORDER BY
    D_YEAR,
    S_NATION,
    P_CATEGORY