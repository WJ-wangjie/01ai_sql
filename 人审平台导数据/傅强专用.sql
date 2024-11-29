-- 查询一审人审工作量
select *
from (
select t.reviewer_id,t.biz_type, count(1), date_format(review_time, '%Y-%m-%d') as dd,1 as type
from (select a.*, b.review_time,b.reviewer_id
      from (select *
            from (select *
                  from tbl_first_message_reviewed where report_time>='2024-11-05'
                  ) as `tfmr*tfmrb0513*`) a
               join (select *
                     from tbl_first_approval where create_time >='2024-11-05'
                     ) b on a.id = b.message_id and b.reviewer_id  not in (29,59)) t
group by reviewer_id,t.biz_type, date_format(review_time, '%Y-%m-%d')
order by biz_type desc,date_format(review_time, '%Y-%m-%d') desc)as a

union all

-- 查询二审人审工作量
select * from (
select t.reviewer_id,t.biz_type, count(1), date_format(review_time, '%Y-%m-%d') as dd,2 as type
from (select a.*, b.review_time,b.reviewer_id
      from (select *
            from tbl_second_message_reviewed where report_time>='2024-11-05') a
               join (select *
                     from tbl_second_approval  where create_time >='2024-11-05'
                     ) b on a.id = b.message_id and b.reviewer_id  not in (29,59)) t
group by reviewer_id,t.biz_type, date_format(review_time, '%Y-%m-%d')
order by biz_type desc,date_format(review_time, '%Y-%m-%d') desc)b order by type,biz_type,dd;





-- 内部

select *
from tbl_first_message_reviewed where id in (
select tbl_first_approval.message_id
from tbl_first_approval where reviewer_id=59);


select t.biz_type, count(1), date_format(review_time, '%Y-%m-%d')
from (select a.*, b.review_time
      from (select *
            from (select *
                  from tbl_first_message_reviewed
#                   union all
#                   select *
#                   from tbl_first_message_reviewed_before_0513
                  ) as `tfmr*tfmrb0513*`) a
               join (select *
                     from tbl_first_approval
#                      union all
#                      select *
#                      from tbl_first_approval_before_0420
#                      union all
#                      select *
#                      from tbl_first_approval_before_0417
                     ) b on a.id = b.message_id and b.reviewer_id not in (29,59)
                                and b.reviewer_id in (4,5,6, 7, 18, 44, 52)
      ) t
where t.biz_type='open-api'
group by t.biz_type, date_format(review_time, '%Y-%m-%d')
order by biz_type desc,date_format(review_time, '%Y-%m-%d') desc;





-- 查询二审人审工作量
select t.biz_type, count(1), date_format(review_time, '%Y-%m-%d')
from (select a.*, b.review_time
      from (select *
            from tbl_second_message_reviewed) a
               join (select *
                     from tbl_second_approval
                     ) b on a.id = b.message_id and b.reviewer_id  not in (29,59) and b.reviewer_id in (4,5,6, 7, 18, 44, 52)) t
where t.biz_type='open-api'
group by t.biz_type, date_format(review_time, '%Y-%m-%d')
order by biz_type desc,date_format(review_time, '%Y-%m-%d') desc;



select  count(1), date_format(review_time, '%Y-%m-%d'),reviewer_id
from (select a.*, b.review_time,reviewer_id
      from (select *
            from tbl_second_message_reviewed) a
               join (select *
                     from tbl_second_approval
                     ) b on a.id = b.message_id and b.reviewer_id != 29 and b.reviewer_id in (4,5,6, 7, 18, 44, 52)) t
group by date_format(review_time, '%Y-%m-%d'),reviewer_id
order by date_format(review_time, '%Y-%m-%d') desc;


select *
from sys_user where id in (4,5,6, 7, 18, 44, 52);


SELECT
            tfa.reviewer_id as review_man_id,
            DATE_FORMAT(tfa.create_time,'%Y-%m-%d') as statistics_time,
            tfaqr.biz_type,
            count(1) as review_count
        from tbl_second_approval tfa
            LEFT JOIN tbl_second_approval_queue_reviewed tfaqr
        on tfa.message_id=tfaqr.message_id
        where create_time>='2024-06-04 00:00:00' and create_time<='2024-06-04 23:59:59'
        GROUP BY tfa.reviewer_id,tfaqr.biz_type,DATE_FORMAT(tfa.create_time,'%Y-%m-%d') ORDER BY reviewer_id desc;

select *
from tbl_review_team  where review_man_id=4 and statistics_time='2024-06-04 00:00:00' and review_number=2;

select *
from tbl_second_approval_queue_reviewed where tbl_second_approval_queue_reviewed.message_id in (select message_id
from tbl_second_approval where reviewer_id=4 and create_time>='2024-06-04 00:00:00' and create_time<='2024-06-04 23:59:59'
);
select *
from tbl_second_approval tfa
            LEFT JOIN tbl_second_approval_queue_reviewed tfaqr on tfa.message_id=tfaqr.message_id
where reviewer_id=4 and create_time>='2024-06-04 00:00:00' and create_time<='2024-06-04 23:59:59'

