-- 查询一审人审工作量
select t.biz_type, count(1), date_format(review_time, '%Y-%m-%d')
from (select a.*, b.review_time
      from (select *
            from tbl_first_message_reviewed) a
               join (select *
                     from tbl_first_approval
                     union all
                     select *
                     from tbl_first_approval_before_0420
                     union all
                     select *
                     from tbl_first_approval_before_0417) b on a.id = b.message_id and b.reviewer_id != 29) t
group by t.biz_type, date_format(review_time, '%Y-%m-%d')
order by biz_type desc,date_format(review_time, '%Y-%m-%d') desc;

-- 查询二审人审工作量
select t.biz_type, count(1), date_format(review_time, '%Y-%m-%d')
from (select a.*, b.review_time
      from (select *
            from tbl_second_message_reviewed) a
               join (select *
                     from tbl_second_approval
                     ) b on a.id = b.message_id and b.reviewer_id != 29) t
group by t.biz_type, date_format(review_time, '%Y-%m-%d')
order by biz_type desc,date_format(review_time, '%Y-%m-%d') desc;