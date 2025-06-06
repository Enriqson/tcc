SELECT
    D_YEAR,
    C_NATION,
    sum(LO_REVENUE - LO_SUPPLYCOST) AS PROFIT
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
    AND (P_MFGR = 'MFGR#1' OR P_MFGR = 'MFGR#2')
GROUP BY
    D_YEAR,
    C_NATION
ORDER BY
    D_YEAR,
    C_NATION