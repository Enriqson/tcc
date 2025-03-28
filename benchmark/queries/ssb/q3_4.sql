SELECT
    C_CITY,
    S_CITY,
    D_YEAR,
    sum(LO_REVENUE) AS revenue
FROM
    {customer_table},
    {lineorder_table},
    {supplier_table},
    {date_table}
WHERE
    LO_CUSTKEY = C_CUSTKEY
    AND LO_SUPPKEY = S_SUPPKEY
    AND LO_ORDERDATE = D_DATEKEY
    AND (C_CITY='UNITED KI1' OR C_CITY='UNITED KI5')
    AND (S_CITY='UNITED KI1' OR S_CITY='UNITED KI5')
    AND D_YEARMONTH = 'Dec1997'
GROUP BY
    C_CITY,
    S_CITY,
    D_YEAR
ORDER BY
    D_YEAR ASC,
    revenue DESC;