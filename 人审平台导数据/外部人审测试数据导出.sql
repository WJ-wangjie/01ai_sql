set @times = DATE_SUB(CURDATE(), INTERVAL 1 DAY) + INTERVAL 19 HOUR;
select name,
    task_type,
    a.create_time,
       task_id,
       role,
       content,
       risk_result as robot_result,
       case review_status
           when
               1 then 'pending'
           when 2 then 'safe'
           when 3 then 'unsafe'
           when '4' then '超时'
           when '5' then '存疑'
           when '8' then '敏感'
           else review_status end as review_status
from (
select mm.id as mid, mm.task_type, mm.task_id, mm.time_stamp, mm.env, mm.origin_data, mm.origin_result, mm.trace_id, mm.bypass, mm.session_id, mm.account_id, mm.role, mm.biz_type, mm.biz_account_id, mm.biz_client_ip, mm.user_client_ip, mm.scene_type, mm.area, mm.content, mm.message_id, mm.serial_id, mm.is_stream_stop, mm.risk_result, mm.origin_risk_detail, mm.review_level, mm.risk_label, mm.risk_action, mm.origin_risk_action_detail, mm.origin_risk_extra_data, mm.stop_auditor, mm.received_time, mm.escalation, mm.report_status, mm.report_time, mm.delete_status,
       tfa.id,  reviewer_id, review_status, review_comment, risk_label_code, risk_action_code, review_time, review_end_time, create_user_id, update_user_id, create_time, update_time
       ,
       1 AS source
from tbl_first_message_reviewed mm
         join grade_risk.tbl_first_approval tfa on tfa.message_id = mm.id
    and tfa.reviewer_id in (42,43,49,50,51) and create_time>=@times
union all
select mm.id as mid, mm.task_type, mm.task_id, mm.time_stamp, mm.env, mm.origin_data, mm.origin_result, mm.trace_id, mm.bypass, mm.session_id, mm.account_id, mm.role, mm.biz_type, mm.biz_account_id, mm.biz_client_ip, mm.user_client_ip, mm.scene_type, mm.area, mm.content, mm.message_id, mm.serial_id, mm.is_stream_stop, mm.risk_result, mm.origin_risk_detail, mm.review_level, mm.risk_label, mm.risk_action, mm.origin_risk_action_detail, mm.origin_risk_extra_data, mm.stop_auditor, mm.received_time, mm.escalation, mm.report_status, mm.report_time, mm.delete_status,
       tfa.id,  reviewer_id, review_status, review_comment, risk_label_code, risk_action_code, review_time, review_end_time, create_user_id, update_user_id, create_time, update_time
     , 2 AS source
from tbl_second_message_reviewed mm
         join grade_risk.tbl_second_approval tfa on tfa.message_id = mm.id
    and tfa.reviewer_id in (42,43,49,50,51) and create_time>=@times
union all
select mm.id as mid, mm.task_type, mm.task_id, mm.time_stamp, mm.env, mm.origin_data, mm.origin_result, mm.trace_id, mm.bypass, mm.session_id, mm.account_id, mm.role, mm.biz_type, mm.biz_account_id, mm.biz_client_ip, mm.user_client_ip, mm.scene_type, mm.area, mm.content, mm.message_id, mm.serial_id, mm.is_stream_stop, mm.risk_result, mm.origin_risk_detail, mm.review_level, mm.risk_label, mm.risk_action, mm.origin_risk_action_detail, mm.origin_risk_extra_data, mm.stop_auditor, mm.received_time, mm.escalation, mm.report_status, mm.report_time, mm.delete_status,
       tfa.id,  reviewer_id, review_status, review_comment, risk_label_code, risk_action_code, review_time, review_end_time, create_user_id, update_user_id, create_time, update_time
     , 3 AS source
from tbl_third_message_reviewed mm
         join grade_risk.tbl_third_approval tfa on tfa.message_id = mm.id
    and tfa.reviewer_id in (42,43,49,50,51) and create_time>=@times
)a join sys_user b on a.reviewer_id=b.id
where a.create_time >= DATE_SUB(CURDATE(), INTERVAL 1 DAY) + INTERVAL 19 HOUR  and   a.create_time <= DATE_SUB(CURDATE(), INTERVAL 0 DAY) + INTERVAL 19 HOUR
order by name,a.create_time;



select count(1)
from tbl_first_approval
where reviewer_id in (43)
  and create_time >= DATE_SUB(CURDATE(), INTERVAL 1 DAY) + INTERVAL 0 HOUR
  and create_time <= DATE_SUB(CURDATE(), INTERVAL 0 DAY) + INTERVAL 0 HOUR;