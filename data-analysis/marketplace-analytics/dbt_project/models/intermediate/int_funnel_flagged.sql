WITH stg_events AS (
    SELECT * FROM {{ ref('stg_events') }}
)

SELECT
    user_id,
    user_session,
    DATE_TRUNC('day', event_time)::DATE AS event_date, 
    variant_user,
    variant_session,

    MAX(CASE WHEN event_name = 'view_item'     THEN 1 ELSE 0 END) AS viewed,
    MAX(CASE WHEN event_name = 'add_to_cart'   THEN 1 ELSE 0 END) AS added_to_cart,
    MAX(CASE WHEN event_name = 'purchase_item' THEN 1 ELSE 0 END) AS purchased,
    COUNT(*) AS total_events

FROM stg_events
GROUP BY
    user_id,
    user_session,
    event_date,
    variant_user,
    variant_session


/*
SMOKE TESTS:
-- On intermediate view
SELECT COUNT(DISTINCT user_id), COUNT(DISTINCT user_session)
FROM intermediate.int_funnel_flagged;

-- On raw (same logic, no grouping needed)
SELECT COUNT(DISTINCT user_id), COUNT(DISTINCT user_session)
FROM raw.events;
*/