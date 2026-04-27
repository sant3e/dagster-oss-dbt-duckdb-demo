-- Static category taxonomy — seeded via `dbt seed`.
SELECT
    ID::VARCHAR AS ID,
    CAT::VARCHAR AS CAT,
    SUBCAT::VARCHAR AS SUBCAT,
    MAINTENANCE::VARCHAR AS MAINTENANCE
FROM {{ ref('PX_CAT_G1V2') }}
