SELECT
    user_id,
    user_session,
    product_id,
    event_time,
    event_type,

    -- Normalize event naming (important for storytelling later)
    CASE 
        WHEN event_type = 'view' THEN 'view_item'
        WHEN event_type = 'cart' THEN 'add_to_cart'
        WHEN event_type = 'purchase' THEN 'purchase_item'
        ELSE event_type
    END AS event_name,

    -- User-level variants
    CASE 
        WHEN user_id % 2 = 0 THEN 'A'
        ELSE 'B'
    END AS variant_user,

    -- Session-level variants
    CASE 
        WHEN ('0x' || right(user_session::text, 1))::int % 2 = 0 THEN 'A'     -- Convert last character of uuid to its hexadecimal representation, then split on odd/even
        ELSE 'B'
    END AS variant_session,


    -- Pulled from raw events so it can feed segments.sql downstream. Other columns could be pulled on a need basis
    brand,
    category_code

FROM {{ source('raw', 'events') }}