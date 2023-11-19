-- auto-generated definition
create table review_request_1119
(
    id          int auto_increment
        primary key,
    prompt      varchar(500)  null comment '入参 input或output',
    prompt_type varchar(1000) null comment '问题分类',
    prompt_base varchar(2000) null comment '题库类型 1 正规题库1 2 正规题库2 3 昊天镜题库 4 安全侧自有题库',
    data_type   varchar(200)  null comment ' input output',
    htj_need    int default 1 null comment '是否需要跑数 1 需要 2 不需要',
    sm_need     int default 1 null comment '是否需要跑数 1 需要 2 不需要',
    r3_need     int default 1 null comment '是否需要跑数 1 需要 2 不需要',
    r4_need     int default 1 null comment '是否需要跑数 1 需要 2 不需要',
    bd_need     int default 1 null comment '是否需要跑数 1 需要 2 不需要',
    al_need     int default 1 null comment '是否需要跑数 1 需要 2 不需要',
    label       varchar(200)  null comment '标签',
    sheet       varchar(200)  null comment '第几个sheet 数据来源'
)
    comment '评测评审请求表';


-- auto-generated definition
create table review_response_1119
(
    id                 int auto_increment
        primary key,
    api_type           varchar(500)  null comment '接口类型',
    label              varchar(200)  null comment '标签',
    prompt             varchar(500)  null comment '入参 input或output',
    prompt_type        varchar(1000) null comment '问题分类',
    prompt_base        varchar(2000) null comment '题库类型 1 正规题库1 2 正规题库2 3 昊天镜题库 4 安全侧自有题库',
    data_type          varchar(200)  null comment ' input output',
    response           text          null comment '原始响应',
    origin_response    text          null comment '原始响应',
    risk_result        text          null comment '是否安全',
    risk_action_detail text          null comment 'risk_action_detail',
    extra_data         text          null comment 'extra_data',
    batch              varchar(500)  null comment '批次+时间'
)
    comment '评测评审响应表';



-- auto-generated definition
create table person_1119
(
    trace_id           text         null,
    timestamp_raise3   text         null,
    content_raise3     text         null,
    rist_result_raise3 text         null,
    r3审核员1          text         null,
    r3审核员2          text         null,
    timestamp_raise4   text         null,
    content_raise4     text         null,
    rist_result_raise4 text         null,
    r4审核员1          text         null,
    r4审核员2          text         null,
    isUnsafe           varchar(500) null
);

