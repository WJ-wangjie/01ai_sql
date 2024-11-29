SET @start_time = 1727712000000;
WITH merged_data AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY source DESC) AS row_num
    FROM (
#     SELECT *, 1 AS source FROM tbl_first_message_reviewed where time_stamp>=@start_time
#         UNION ALL
        SELECT *, 2 AS source FROM tbl_second_message_reviewed where time_stamp>=@start_time
#         UNION ALL
#         SELECT *, 3 AS source FROM tbl_third_message_reviewed where time_stamp>=@start_time
    ) AS combined_tables
)
select *
from (
SELECT
                JSON_UNQUOTE(JSON_EXTRACT(JSON_UNQUOTE(JSON_EXTRACT(report_json, '$.data.source_data')), '$.valm_flag')) AS valm_flag,
                JSON_UNQUOTE(JSON_EXTRACT(JSON_UNQUOTE(JSON_EXTRACT(report_json, '$.data.source_data')), '$.risk_result')) AS robot_result,
                JSON_UNQUOTE(JSON_EXTRACT(JSON_UNQUOTE(JSON_EXTRACT(report_json, '$.data.source_data')), '$.origin_result')) AS robot_origin_result,
                JSON_UNQUOTE(JSON_EXTRACT(report_json, '$.data.review_result')) AS person_result,
                (JSON_EXTRACT(report_json, '$.data.risk_detail.risk_label')) AS person_risk_lable,
                JSON_EXTRACT(JSON_UNQUOTE(JSON_EXTRACT(report_json, '$.data.source_data')), '$.risk_detail.risk_label') AS robot_risk_lable,
                JSON_UNQUOTE(JSON_EXTRACT(JSON_UNQUOTE(JSON_EXTRACT(report_json, '$.data.source_data')), '$.role')) AS role,
                JSON_UNQUOTE(JSON_EXTRACT(JSON_UNQUOTE(JSON_EXTRACT(report_json, '$.data.source_data')), '$.risk_detail.risk_action')) AS risk_action,
                JSON_UNQUOTE(JSON_EXTRACT(JSON_UNQUOTE(JSON_EXTRACT(report_json, '$.data.source_data')), '$.scene_type')) AS scene_type,
                JSON_UNQUOTE(JSON_EXTRACT(report_json, '$.env'))  AS env,
                JSON_UNQUOTE(JSON_EXTRACT(JSON_UNQUOTE(JSON_EXTRACT(report_json, '$.data.source_data')), '$.trace_id')) AS trace_id,
                JSON_UNQUOTE(JSON_EXTRACT(report_json, '$.task_id')) AS task_id,
                JSON_EXTRACT(JSON_UNQUOTE(JSON_EXTRACT(report_json, '$.data.source_data')), '$.risk_detail.review_level') AS review_level,
                b.content,
                JSON_UNQUOTE(JSON_EXTRACT(JSON_UNQUOTE(JSON_EXTRACT(report_json, '$.data.source_data')), '$.risk_detail.stop_auditor')) AS stop_auditor,
                from_unixtime(round(b.time_stamp/1000),'%Y-%m-%d %H:%i:%s') as robot_time,
                a.report_time AS person_time,
                JSON_UNQUOTE(JSON_EXTRACT(JSON_UNQUOTE(JSON_EXTRACT(report_json, '$.data.source_data')), '$.biz_type')) AS biz_type,
                JSON_UNQUOTE(JSON_EXTRACT(JSON_UNQUOTE(JSON_EXTRACT(report_json, '$.data.source_data')), '$.user.account_id')) AS account_id,
               report_json
        FROM tbl_risk_report a left join (SELECT * FROM merged_data WHERE row_num = 1) b   on a.message_id = b.id
) aa where robot_time >= '2024-10-22 00:00:00' and robot_time <= '2024-10-30 23:59:59' and   person_time<=  DATE_SUB(CURDATE(), INTERVAL 0 DAY) + INTERVAL 12 HOUR
#        and biz_type = 'arm'
#        and biz_type = 'dotline_search'
       and biz_type in ('wanzhi','open-api','playground','digital_human','dotline_search')
#        and biz_type in ('wanzhi','open-api','legacy','playground')
       and (risk_action != 'redline' or risk_action is null or  risk_action = '')
#        and robot_result='safe' and person_result='unsafe';
       and robot_result!=person_result order by robot_time ;


select JSON_UNQUOTE(JSON_EXTRACT(JSON_UNQUOTE(JSON_EXTRACT(report_json, '$.data.source_data')), '$.valm_flag')) ,count(1)
from tbl_risk_report group by JSON_UNQUOTE(JSON_EXTRACT(JSON_UNQUOTE(JSON_EXTRACT(report_json, '$.data.source_data')), '$.valm_flag')) ;