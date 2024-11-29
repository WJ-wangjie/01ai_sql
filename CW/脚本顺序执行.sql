# 更新add_time数据
update transaction_log set add_time=target_date where target_date is not null and handle=1 and target_date >='2024-07-01 00:00:00'and target_date <'2024-08-01 00:00:00' ;

# 更新token数据
update transaction_log set promptToken= CASE
           WHEN ext_info IS NULL OR ext_info = '' THEN 0
           WHEN JSON_VALID(ext_info) = 0 THEN 0
           ELSE JSON_EXTRACT(ext_info, '$.tokenUsage.prompt_tokens')
           END,
                           completionToken = CASE
           WHEN ext_info IS NULL OR ext_info = '' THEN 0
           WHEN JSON_VALID(ext_info) = 0 THEN 0
           ELSE JSON_EXTRACT(ext_info, '$.tokenUsage.completion_tokens')
           END,
                           totalToken =
       CASE
           WHEN ext_info IS NULL OR ext_info = '' THEN 0
           WHEN JSON_VALID(ext_info) = 0 THEN 0
           ELSE JSON_EXTRACT(ext_info, '$.tokenUsage.total_tokens')
           END
    where handle=1 and target_date is not null and target_date >='2024-07-01 00:00:00'and target_date <'2024-08-01 00:00:00' ;


# 根据实时数据计算token消耗
REPLACE INTO app_tokens_every_day (app_id, model, dt, prompt_tokens, completion_tokens, request_count, add_time, update_time)
select  app_id, 'model-test' as model, date_format(add_time,'%Y-%m-%d') as dt, sum(promptToken) as  prompt_tokens, sum(completionToken) as completion_tokens, count(1) as request_count, CONCAT(DATE_FORMAT(DATE_ADD(add_time, INTERVAL 1 DAY), '%Y-%m-%d'), ' 00:05:00')  as add_time, CONCAT(DATE_FORMAT(DATE_ADD(add_time, INTERVAL 1 DAY), '%Y-%m-%d'), ' 00:05:00') as update_time
from transaction_log where add_time > '2024-07-01 00:05:00' and add_time <='2024-08-01 00:10:00' group by date_format(add_time,'%Y-%m-%d');

# 计算账单
REPLACE INTO transaction_bill_daily (bill_date, user_id, app_id, model, trans_type, total_tokens_usage, in_amount, out_amount, before_balance, after_balance, add_time, update_time)
select date_format(dt,'%Y%m%d')   bill_date, '661e40b89a5d89ef21721a72' as user_id, 'e3ec43686e9e4475b595c1924296e003' as app_id, model,
       0 as trans_type, (prompt_tokens+completion_tokens) as total_tokens_usage, 0 as in_amount, -(prompt_tokens+completion_tokens)/1000000*2.1*1000000000 as out_amount, 0 as before_balance, 0 as after_balance, add_time, update_time
from app_tokens_every_day  where add_time >'2024-07-01 00:05:00'and add_time <='2024-08-01 00:05:00' ;

#查询花了多少钱了 <0 结果为1 ture 就说明已经花够钱了
select 1200+(round(sum(out_amount)/1000000000))<0
from transaction_bill_daily where add_time >'2024-07-01 00:05:00'and add_time <='2024-08-01 00:05:00' ;

select (round(sum(out_amount)/1000000000))
from transaction_bill_daily where add_time >'2024-07-01 00:05:00'and add_time <='2024-08-01 00:05:00' ;

#查询账户余额
select *
from account_balance;

#查询多少条了
select count(1)
from transaction_log;

select *
from transaction_log;
