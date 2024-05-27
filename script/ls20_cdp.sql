use cdp;


drop table cdp_user_tag_bitmap_local on cluster default;
CREATE TABLE cdp_user_tag_bitmap_local on cluster default
(
    tag_id         UInt64 COMMENT 'tag唯一标识符',
    tag_value      String COMMENT 'tag值',
    tag_name       String COMMENT 'tag名称',
    unique_user_id AggregateFunction(groupBitmap,UInt64) COMMENT '使用位图存储用户全局唯一ID，ONE-ID',
    tag_category   UInt8 COMMENT 'tag分类'
    ) ENGINE = AggregatingMergeTree()
    PARTITION BY tag_id
    ORDER BY (tag_id,tag_value,tag_name,tag_category);

drop table cdp_user_tag_bitmap_all on cluster default;
CREATE TABLE cdp_user_tag_bitmap_all on cluster default as cdp_user_tag_bitmap_local
    ENGINE = Distributed(default, 'cdp', 'cdp_user_tag_bitmap_local', tag_id);

INSERT INTO cdp_user_tag_bitmap_all (tag_id, tag_value, tag_name, unique_user_id, tag_category)
SELECT
    tag_id,
    tag_value,
    tag_name,
    bitmapBuild(cast([unique_user_id] as Array(UInt64))),
    tag_category
FROM cdp_user_tag_all
GROUP BY tag_id, tag_value, tag_name, tag_category, unique_user_id;


--bitmap
INSERT INTO cdp_user_tag_bitmap_all (tag_id, tag_value, tag_name, unique_user_id, tag_category)
SELECT
    5,
    '高端手机',
    '手机偏好',
    bitmapBuild(cast([unique_user_id] as Array(UInt64))),
    5
FROM cdp_user_event_all
WHERE event_type = 'browse'
  AND page_id IN (1100442, 1749628, 1960722, 1869590, 1674494)

--高端手机用户查询

SELECT arrayJoin(bitmapToArray(bitmapAnd(bitmapAnd(bitmapAnd(user_0,user_1),user_2),user_3))) as user_list
FROM
    (
        SELECT 1 as join_id, groupBitmapMergeState(unique_user_id) as user_0
        FROM cdp_user_tag_bitmap_all
        WHERE tag_id = 4 and tag_value = '2'
    ) t0
        INNER JOIN
    (
        SELECT 1 as join_id, groupBitmapMergeState(unique_user_id) as user_1
        FROM cdp_user_tag_bitmap_all
        WHERE tag_id = 1
          AND tag_value = '中等消费能力'
    ) AS t1 ON t0.join_id = t1.join_id
        INNER JOIN
    (
        SELECT 1 as join_id, groupBitmapMergeState(unique_user_id) as user_2
        FROM cdp_user_tag_bitmap_all
        WHERE tag_id = 2
          AND tag_value = '高活跃度'
    ) AS t2 ON t0.join_id = t2.join_id
        INNER JOIN
    (
        SELECT 1 as join_id, groupBitmapMergeState(unique_user_id) as user_3
        FROM cdp_user_tag_bitmap_all
        WHERE tag_id = 5
          AND tag_value = '高端手机'
    ) AS t3 ON t0.join_id = t3.join_id;
