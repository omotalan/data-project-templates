WITH stg_events AS (
    SELECT
        user_id,
        max(variant) AS variant
    FROM {{ ref('stg_events') }}
    GROUP BY 1
),

fct_funnel AS (
    SELECT * FROM {{ ref('fct_funnel') }}
)

SELECT
    se.variant,
    count(distinct se.user_id) AS users,
    sum(ff.contacted) AS conversions,
    -- Force float division
    sum(ff.contacted) * (1.0 / count(distinct se.user_id)) AS user_conversion_rate,
    sum(ff.contacted) * (1.0 / sum(ff.viewed)) AS view_conversion_rate
FROM stg_events se
JOIN fct_funnel ff ON ff.user_id =  se.user_id
GROUP BY 1