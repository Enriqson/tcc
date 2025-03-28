SELECT
    C_CITY,
    S_CITY,
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
    AND C_NATION = 'UNITED STATES'
    AND S_NATION = 'UNITED STATES'
    AND D_YEAR >= 1992 AND D_YEAR <= 1997
GROUP BY
    C_CITY,
    S_CITY,
    D_YEAR
ORDER BY
    D_YEAR ASC,
    REVENUE DESC;