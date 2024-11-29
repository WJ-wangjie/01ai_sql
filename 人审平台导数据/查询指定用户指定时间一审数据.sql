select a.review_status,c.risk_result,a.review_time,c.content,b.username,task_id,trace_id
from sys_user b
join tbl_first_approval a on a.reviewer_id=b.id and b.username  in  ('lidongming@01.ai','xiongyunfeng@01.ai') and review_time>='2024-10-09'
join tbl_first_message_reviewed c on a.message_id=c.id order by username,review_time;
