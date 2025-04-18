SELECT
    C_NATION,
    S_NATION,
    D_YEAR,
    sum(LO_REVENUE) AS REVENUE
FROM
    {customer_table},
    {lineorder_table},
    {supplier_table},
    {date_table}
WHERE
    LO_CUSTKEY = C_CUSTKEY
    AND LO_SUPPKEY = S_SUPPKEY
    AND LO_ORDERDATE = D_DATEKEY
    AND C_REGION = 'ASIA' AND S_REGION = 'ASIA'
    AND D_YEAR >= 1992 AND D_YEAR <= 1997
GROUP BY
    C_NATION,
    S_NATION,
    D_YEAR
ORDER BY
    D_YEAR ASC,
    REVENUE DESC;