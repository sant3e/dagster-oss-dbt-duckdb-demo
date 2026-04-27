-- Ported from Snowflake: length(x::VARCHAR) replaces LENGTH(CAST(x AS VARCHAR)).
SELECT
    CASE
        WHEN length(CID::VARCHAR) = 13 THEN SUBSTRING(CID, 4)
        WHEN length(CID::VARCHAR) = 10 THEN CID
        ELSE NULL
    END AS CID,
    CASE
        WHEN BDATE > CURRENT_DATE THEN NULL
        ELSE BDATE
    END AS BDATE,
    CASE
        WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
        WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'
        ELSE 'N/A'
    END AS GEN,
    CURRENT_TIMESTAMP AS dwh_create_date
FROM {{ ref('raw_erp_CUST_AZ12') }}
