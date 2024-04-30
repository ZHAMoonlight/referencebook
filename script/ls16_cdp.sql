create database cdp;
use cdp;

CREATE TABLE cdp_user
(
    unique_user_id  BIGINT NOT NULL COMMENT '用户全局唯一ID，ONE-ID',
    name            VARCHAR(255) COMMENT '用户姓名',
    nickname        VARCHAR(255) COMMENT '用户昵称',
    gender          INT COMMENT '性别：1-男；2-女；3-未知',
    birthday        VARCHAR(8) COMMENT '用户生日：yyyyMMdd',
    user_level      INT COMMENT '用户等级：1-5',
    register_date   DATETIME COMMENT '注册日期',
    last_login_time DATETIME COMMENT '最后一次登录时间'
) PRIMARY KEY (unique_user_id)
DISTRIBUTED BY HASH(unique_user_id)
ORDER BY(`register_date`)
PROPERTIES (
"replication_num" = "1"
);

CREATE TABLE cdp_user_metrics
(
    unique_user_id BIGINT NOT NULL COMMENT '用户全局唯一ID，ONE-ID',
    sum_pay        DECIMAL(20, 10) SUM COMMENT '总计消费金额',
    sum_pay_num    INT SUM COMMENT '总计消费次数',
    sum_30d_login  INT SUM COMMENT '30天登录次数'
) AGGREGATE KEY(unique_user_id)
  DISTRIBUTED BY HASH(unique_user_id)
  PROPERTIES (
  "replication_num" = "1"
  );

CREATE TABLE cdp_user_event
(
    event_time     DATETIME COMMENT '事件发生时间',
    event_type     VARCHAR(50) COMMENT '事件类型，pay,add_shop_cat,browse,recharge等',
    unique_user_id BIGINT NOT NULL COMMENT '用户全局唯一ID，ONE-ID',
    order_no VARCHAR(64) COMMENT '订单唯一编号',
    page_id        INT COMMENT '浏览事件页面ID',
    item_id        ARRAY< INT > COMMENT '浏览、加购、下单事件中商品ID',
    total_amount   DECIMAL(18, 2) COMMENT '订单金额',
    device_type    VARCHAR(50) COMMENT '设备类型',
    event_param    JSON COMMENT '事件相关参数，比如购买事件商品ID、支付金额等',
    location       VARCHAR(100) COMMENT '发生地点，如城市、门店等'
) DUPLICATE KEY(event_time,event_type)
PARTITION BY date_trunc('day', event_time)
DISTRIBUTED BY HASH(event_type,unique_user_id)
PROPERTIES (
"replication_num" = "1"
);

CREATE TABLE cdp_user_tag
(
    tag_id         BIGINT       NOT NULL COMMENT 'tag唯一标识符',
    tag_value      VARCHAR(128) NOT NULL COMMENT 'tag值',
    tag_name       VARCHAR(32)  NOT NULL COMMENT 'tag名称',
    unique_user_id BIGINT       NOT NULL COMMENT '用户全局唯一ID，ONE-ID',
    tag_category   INT          NOT NULL COMMENT 'tag分类'
) DUPLICATE KEY(tag_id,tag_value)
PARTITION BY (tag_id)
DISTRIBUTED BY HASH(tag_value,unique_user_id)
PROPERTIES (
"replication_num" = "1"
);
