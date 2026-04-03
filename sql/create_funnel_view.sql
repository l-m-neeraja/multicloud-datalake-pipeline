CREATE OR REPLACE VIEW funnel_analysis AS
SELECT
    COUNT(DISTINCT CASE WHEN event_type = 'page_view' THEN user_id END) AS total_page_views_users,
    COUNT(DISTINCT CASE WHEN event_type = 'product_view' THEN user_id END) AS total_product_views_users,
    COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN user_id END) AS total_add_to_cart_users,
    COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) AS total_purchases_users
FROM clickstream_gcs.events;
