truncate table  cdp_user_tag_all;

INSERT INTO cdp_user_tag_all (tag_id, tag_value, tag_name, unique_user_id, tag_category)
SELECT
    1 as tag_id,
    CASE
        WHEN total_amount >= 100000 THEN '高消费能力'
        WHEN total_amount >= 50000 THEN '中等消费能力'
        ELSE '低消费能力'
        END AS tag_value,
    '消费能力标签' AS tag_name,
    unique_user_id,
    1 AS tag_category
FROM (
         SELECT e.unique_user_id, SUM(e.total_amount) AS total_amount
         FROM cdp_user_event_all e
         GROUP BY e.unique_user_id
     ) AS subquery;


INSERT INTO cdp_user_tag_all (tag_id, tag_value, tag_name, unique_user_id, tag_category)
SELECT
    2 AS col1,
    CASE
        WHEN sumIf(DATEDIFF('day', now(), event_time) <= 60, 1) >= 10 THEN '高活跃度'
        WHEN sumIf(DATEDIFF('day', now(), event_time) <= 60, 1) >= 5 THEN '中等活跃度'
        ELSE '低活跃度'
        END AS tag_value,
    '活跃度标签' AS tag_name,
    unique_user_id AS col4,
    2 AS tag_category
FROM cdp.cdp_user_event_all
GROUP BY unique_user_id;


INSERT INTO cdp_user_tag_all (tag_id, tag_value, tag_name, unique_user_id, tag_category)
SELECT 3,
       location,
       '位置' AS tag_name,
       e.unique_user_id,
       3 AS tag_category
FROM cdp_user_event_all e;


INSERT INTO cdp_user_tag_all (tag_id, tag_value, tag_name, unique_user_id, tag_category)
SELECT 4,
       gender,
       '性别' AS tag_name,
       u.unique_user_id,
       4 AS tag_category
FROM cdp_user_all u;
