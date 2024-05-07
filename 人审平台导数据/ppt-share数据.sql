WITH merged_data AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY source DESC) AS row_num
    FROM (
        SELECT *, 2 AS source FROM tbl_second_message_reviewed
        UNION ALL
        SELECT *, 3 AS source FROM tbl_third_message_reviewed
    ) AS combined_tables
)
select *
from (
SELECT
    b.source,
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
#                 JSON_UNQUOTE(JSON_EXTRACT(JSON_UNQUOTE(JSON_EXTRACT(report_json, '$.data.source_data')), '$.content')) AS content,
                b.content,
                JSON_UNQUOTE(JSON_EXTRACT(JSON_UNQUOTE(JSON_EXTRACT(report_json, '$.data.source_data')), '$.risk_detail.stop_auditor')) AS stop_auditor,
                from_unixtime(round(b.time_stamp/1000),'%Y-%m-%d %H:%i:%s') as robot_time,
                a.report_time AS person_time,
                JSON_UNQUOTE(JSON_EXTRACT(JSON_UNQUOTE(JSON_EXTRACT(report_json, '$.data.source_data')), '$.biz_type')) AS biz_type,
                JSON_UNQUOTE(JSON_EXTRACT(JSON_UNQUOTE(JSON_EXTRACT(report_json, '$.data.source_data')), '$.user.account_id')) AS account_id,
               report_json
        FROM (select *
from tbl_risk_report
union all
select *
from tbl_risk_report_before0417
union all
select *
from tbl_risk_report_before_04_20) a left join (select * from merged_data where row_num=1) b   on a.message_id = b.id
) aa where robot_time >= '2024-04-01 00:00:00'
       and scene_type='ppt_share';



