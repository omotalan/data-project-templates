SELECT
    variant_user,
    count(distinct user_id) AS users,
    sum(purchased) AS conversions,
    -- Force float division
    sum(purchased) * (1.0 / count(distinct user_id)) AS user_conversion_rate,
    sum(purchased) * (1.0 / sum(viewed)) AS view_conversion_rate
FROM {{ ref('fct_funnel_user') }}
GROUP BY 1