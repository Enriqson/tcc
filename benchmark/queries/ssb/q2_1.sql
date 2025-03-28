SELECT
    sum(LO_REVENUE),
    D_YEAR,
    P_BRAND
FROM
    {lineorder_table},
    {date_table},
    {part_table},
    {supplier_table}
WHERE
    LO_ORDERDATE = D_DATEKEY
    AND LO_PARTKEY = P_PARTKEY
    AND LO_SUPPKEY = S_SUPPKEY
    AND P_CATEGORY = 'MFGR#12'
    AND S_REGION = 'AMERICA'
GROUP BY
    D_YEAR,
    P_BRAND
ORDER BY
    D_YEAR,
    P_BRAND;