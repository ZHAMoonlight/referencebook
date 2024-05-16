create database cdp on cluster default;
use cdp;

CREATE TABLE cdp_user_local on cluster default(
  unique_user_id UInt64 COMMENT '用户全局唯一ID，ONE-ID',
  name String COMMENT '用户姓名',
  nickname String COMMENT '用户昵称',
  gender Int8 COMMENT '性别：1-男；2-女；3-未知',
  birthday String COMMENT '用户生日：yyyyMMdd',
  user_level Int8 COMMENT '用户等级：1-5',
  register_date DateTime64 COMMENT '注册日期',
  last_login_time DateTime64 COMMENT '最后一次登录时间'
) ENGINE = MergeTree()
    PARTITION BY register_date
    PRIMARY KEY unique_user_id
    ORDER BY unique_user_id;

CREATE TABLE cdp_user_all on cluster default as cdp_user_local
    ENGINE = Distributed(default, 'cdp', 'cdp_user_local', unique_user_id);

CREATE TABLE cdp_user_metrics_local on cluster default
(
    unique_user_id UInt64 COMMENT '用户全局唯一ID，ONE-ID',
    sum_pay        DECIMAL(20, 10)  COMMENT '总计消费金额',
    sum_pay_num    UInt16 COMMENT '总计消费次数',
    sum_30d_login  UInt16 COMMENT '30天登录次数'
    )
    ENGINE = SummingMergeTree()
    ORDER BY unique_user_id;

CREATE TABLE cdp_user_metrics_all on cluster default as cdp_user_metrics_local
    ENGINE = Distributed(default, 'cdp', 'cdp_user_metrics_local', unique_user_id);


CREATE TABLE cdp_user_event_local on cluster default
(
    event_time DateTime64 COMMENT '事件发生时间',
    event_type String COMMENT '事件类型，pay,add_shop_cat,browse,recharge等',
    unique_user_id UInt64 NOT NULL COMMENT '用户全局唯一ID，ONE-ID',
    order_no String COMMENT '订单唯一编号',
    page_id UInt64 COMMENT '浏览事件页面ID',
    item_id Array(UInt64) COMMENT '浏览、加购、下单事件中商品ID',
    total_amount DECIMAL(18, 2) COMMENT '订单金额',
    device_type String COMMENT '设备类型',
    event_param String COMMENT '事件相关参数，比如购买事件商品ID、支付金额等',
    location VARCHAR(100) COMMENT '发生地点，如城市、门店等'
    ) ENGINE = MergeTree()
    PARTITION BY date_trunc('day', event_time)
    ORDER BY (event_time,event_type);

CREATE TABLE cdp_user_event_all on cluster default as cdp_user_event_local
    ENGINE = Distributed(default, 'cdp', 'cdp_user_event_local', unique_user_id);

CREATE TABLE cdp_user_tag_local on cluster default
(
    tag_id         UInt64       NOT NULL COMMENT 'tag唯一标识符',
    tag_value      String NOT NULL COMMENT 'tag值',
    tag_name       String NOT NULL COMMENT 'tag名称',
    unique_user_id UInt64       NOT NULL COMMENT '用户全局唯一ID，ONE-ID',
    tag_category   UInt8          NOT NULL COMMENT 'tag分类'
) ENGINE = MergeTree()
    PARTITION BY tag_id
    ORDER BY (tag_id,tag_value);

CREATE TABLE cdp_user_tag_all on cluster default as cdp_user_tag_local
    ENGINE = Distributed(default, 'cdp', 'cdp_user_tag_local', unique_user_id);
