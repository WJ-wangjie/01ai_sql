select *
from transaction_bill_daily where app_id='e3ec43686e9e4475b595c1924296e003'


select *
from account_balance;

select *
from app_tokens_every_day where app_id='e3ec43686e9e4475b595c1924296e003';


select count(
       1
       )
from transaction_log where target_date is not null limit 5000;


select round(sum(totalToken)/1000000*2)
from transaction_log;


select *
from transaction_bill_daily where app_id='e3ec43686e9e4475b595c1924296e003';

# 插入数据 账单的
insert into transaction_bill_daily
select null as id,replace(dt,'-','') as bill_date, '661e40b89a5d89ef21721a72', app_id, model, 0 as trans_type, completion_tokens+prompt_tokens as total_tokens_usage, 0 as in_amount, -1, 0, 0, add_time, update_time
from app_tokens_every_day where app_id='e3ec43686e9e4475b595c1924296e003' and add_time >='2024-07'and add_time <='2024-08-02 00:10:00';

select id, bill_date, user_id, app_id, model, trans_type, total_tokens_usage, in_amount, out_amount, before_balance, after_balance, add_time, update_time
from transaction_bill_daily where app_id='e3ec43686e9e4475b595c1924296e003';

# select 1106964971800/632508560
select 30789811281/1000000*2.1*1000000000
select 22883037977/1000000*2.1*1000000000


select *
from app_tokens_every_day where app_id='e3ec43686e9e4475b595c1924296e003';

select *
from transaction_bill_daily where app_id='e3ec43686e9e4475b595c1924296e003';


### Java代码生成脚本并入库

select count(1),target_date is Null
from transaction_log group by target_date is Null ;

select round(sum(CASE
           WHEN ext_info IS NULL OR ext_info = '' THEN 0
           WHEN JSON_VALID(ext_info) = 0 THEN 0
           ELSE JSON_EXTRACT(ext_info, '$.tokenUsage.total_tokens')
           END AS totalToken)/1000000)*2.1
from transaction_log;

select *
from transaction_log where target_date='2024-07-01 11:02:01';


select target_date is null,count(1)
from transaction_log group by target_date is null;

select count(1)
from transaction_log;

# target_date置为空
update transaction_log set target_date =null where target_date is not null;
#清空7月份数据 在调用明细显示的数据
update transaction_log set add_time =null where add_time >='2024-07-01' and add_time <='2024-09-01';

select *
from transaction_log where biz_no='a2d2855ddbe802291d9b92e1f2dd7ae1';

select *
        from transaction_log where handle=0 and app_id='e3ec43686e9e4475b595c1924296e003' order by rand()
            limit 5000

# 更新add_time数据
update transaction_log set add_time=target_date where target_date is not null;


INSERT INTO data_openapi.transaction_log (id, user_id, app_id, biz_no, trans_type, amount, before_balance,
                                          after_balance, ext_info, add_time, update_time, target_date, promptToken,
                                          completionToken, totalToken, handle)
VALUES (126592835, '661e40b89a5d89ef21721a72', 'e3ec43686e9e4475b595c1924296e003', 'c0a676357d59538130b3ba6a30a25e1c',
        0, -1228500, 170000000000000, 169999998771500,
        '{"modelPrice":{"promptPrice":"0.0021","completionsPrice":"0.0021"},"modelName":"model-test","tokenUsage":{"completion_tokens":563,"prompt_tokens":22,"total_tokens":585}}',
        '2024-11-28 13:31:01', '2024-11-29 14:48:57', '2024-07-01 11:02:01', 22, 563, 585, 1);



# 根据实时数据计算token消耗
REPLACE INTO app_tokens_every_day (app_id, model, dt, prompt_tokens, completion_tokens, request_count, add_time, update_time)
select  app_id, 'model-test' as model, date_format(add_time,'%Y-%m-%d') as dt, sum(promptToken) as  prompt_tokens, sum(completionToken) as completion_tokens, count(1) as request_count, CONCAT(DATE_FORMAT(DATE_ADD(add_time, INTERVAL 1 DAY), '%Y-%m-%d'), ' 00:05:00')  as add_time, CONCAT(DATE_FORMAT(DATE_ADD(add_time, INTERVAL 1 DAY), '%Y-%m-%d'), ' 00:05:00') as update_time
from transaction_log where add_time >='2024-07'and add_time <='2024-08-01 00:10:00' group by date_format(add_time,'%Y-%m-%d');

# 计算账单
REPLACE INTO transaction_bill_daily (bill_date, user_id, app_id, model, trans_type, total_tokens_usage, in_amount, out_amount, before_balance, after_balance, add_time, update_time)
select date_format(dt,'%Y%m%d')   bill_date, '661e40b89a5d89ef21721a72' as user_id, 'e3ec43686e9e4475b595c1924296e003' as app_id, model,
       0 as trans_type, (prompt_tokens+completion_tokens) as total_tokens_usage, 0 as in_amount, -(prompt_tokens+completion_tokens)/1000000*2.1*1000000000 as out_amount, 0 as before_balance, 0 as after_balance, add_time, update_time
from app_tokens_every_day  where add_time >'2024-07-01 00:05:00'and add_time <='2024-08-01 00:05:00' ;


select *
from transaction_bill_daily;

select date_format();

select   CONCAT(DATE_FORMAT(DATE_ADD(target_date, INTERVAL 1 DAY), '%Y-%m-%d'), ' 00:05:00') AS new_datetime
from transaction_log where target_date >='2024-07'and target_date <='2024-08-02 00:10:00' limit 10;

# INSERT INTO data_openapi.app_tokens_every_day (id, app_id, model, dt, prompt_tokens, completion_tokens, request_count,
#                                                add_time, update_time)
# VALUES (null, 'e3ec43686e9e4475b595c1924296e003', 'model-test', '2024-07-26', 1256870826, 1022715558, 1330050,
#         '2024-07-27 00:05:00', '2024-07-27 00:05:00');

REPLACE INTO app_tokens_every_day " +
            "(app_id, model, dt, prompt_tokens, completion_tokens, request_count) " +
            "SELECT app_id, model, #{dt} as dt, sum(prompt_tokens) as prompt_tokens, " +
            "sum(completion_tokens) as completion_tokens, sum(request_count) as request_count " +
            "FROM app_tokens_every_ten_minutes " +
            "WHERE add_time BETWEEN #{startDateTime} and #{endDateTime} " +
            "GROUP BY app_id, model, dt