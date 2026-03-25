SELECT
    variant_session,
    count(distinct user_session) AS user_sessions,
    sum(purchased) AS conversions,
    -- Force float division
    sum(purchased) * (1.0 / count(distinct user_session)) AS session_conversion_rate,
    sum(purchased) * (1.0 / sum(viewed)) AS view_conversion_rate
FROM {{ ref('fct_funnel_session') }}
GROUP BY 1