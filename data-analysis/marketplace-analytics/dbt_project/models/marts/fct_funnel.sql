WITH stg_events AS (
    SELECT * FROM {{ ref('stg_events') }}
)

SELECT
    user_id,
    max(CASE WHEN event_name = 'view_item' THEN 1 ELSE 0 END) AS viewed,
    max(CASE WHEN event_name = 'add_to_cart' THEN 1 ELSE 0 END) AS added_to_cart,
    max(CASE WHEN event_name = 'contact_seller' THEN 1 ELSE 0 END) AS contacted,
    count(*) AS total_events
FROM stg_events
GROUP BY 1