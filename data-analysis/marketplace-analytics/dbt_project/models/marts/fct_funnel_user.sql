WITH stg_events AS (
    SELECT * FROM {{ ref('stg_events') }}
)

SELECT
    user_id,
    variant_user,
    max(CASE WHEN event_name = 'view_item' THEN 1 ELSE 0 END) AS viewed,
    max(CASE WHEN event_name = 'add_to_cart' THEN 1 ELSE 0 END) AS added_to_cart,
    max(CASE WHEN event_name = 'purchase_item' THEN 1 ELSE 0 END) AS purchased,
    count(*) AS total_events
FROM stg_events
GROUP BY 1,2