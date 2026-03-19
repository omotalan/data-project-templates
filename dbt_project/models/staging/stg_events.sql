SELECT
    user_id,
    product_id,
    event_time,
    event_type,

    CASE 
        WHEN user_id % 2 = 0 THEN 'A'
        ELSE 'B'
    END AS variant

FROM {{ source('raw', 'events') }}