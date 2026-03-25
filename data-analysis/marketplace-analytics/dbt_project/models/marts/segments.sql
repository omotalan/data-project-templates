{{ config(materialized='view')}}

WITH user_events AS (
    SELECT
        user_id,
        user_session,
        brand,
        category_code,
        /* Example categorization case: device proxy
        CASE
            WHEN lower(user_agent) LIKE '%mobile% THEN 'mobile
            WHEN lower(user_agent) LIKE '%android% THEN 'mobile
            ELSE 'desktop'
        END AS device_proxy
        */
    FROM {{ ref('stg_events')}}
    GROUP BY 1,2,3,4
)

SELECT * FROM user_events