-- Static ERP customer reference — seeded via `dbt seed`.
SELECT
    CID::VARCHAR AS CID,
    BDATE::DATE AS BDATE,
    GEN::VARCHAR AS GEN
FROM {{ ref('CUST_AZ12') }}
