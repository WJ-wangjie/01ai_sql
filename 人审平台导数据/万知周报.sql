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
from tbl_risk_report_before_04_20
union all
select *
from tbl_risk_report_before0417) a left join (select * from merged_data where row_num=1) b   on a.message_id = b.id
) aa where robot_time >= '2024-04-15 12:00:00' and  robot_time <= '2024-05-01 00:00:00' and person_time<='2024-05-01 00:00:00'
       and (robot_risk_lable != '封建迷信' or robot_risk_lable is null)
       and robot_result!=person_result
           and account_id not in ('65d8a0f2436f3fff9f7ee0f8','65f8feb9f8a083f4e7be27d9',
                                           '65e538c35cb4ae6b37daf6b8',
                                           '65f0219f9e031fcccb315a0b',
                                           '65e5c699dccde69f7f01501a',
                                           '65dc62ad9667c60b18494e81',
                                           '65e0026a091389b8263970b7',
                                           '65f3dacdaf82a808d240b1cf',
                                           '65e5381e209a9afd45adc9ab',
                                           '65e00cf9d0105a9c0cccb249',
                                           '65d6baeb4d20a4e12efc7469',
                                           '65e00a01a567f747142f3e0a',
                                           '65beeed23968b4f5be77fa99',
                                           '65bf3f33364e8594f18e7ec7',
                                           '65b39aaf2fb95bfe215b6f9d',
                                           '659cb09685279e75318ca380',
                                           '65bf3f402d2f54984646c6fa',
                                           '6579b3630a38bab8099698d1',
                                           '6571ae3f48b40d451440580b',
                                           '6579b35f3bddff072105fa27',
                                           '65712d236c4c6110c18629d2',
                                           '657a6a818bb416ea58e7a275',
                                           '65712a23b2e05d4e0d2f77ce',
                                           '6579ce7d578626ce72f35966',
                                           '65708a05414b87d7ef1fa673',
                                           '6551c2dfe9779a8dc8f8b494',
                                           '65ade0a1f5946bf9d6317274',
                                           '65484b49b68d6d4da0a5873e',
                                           '6555c8476ce880431b865ca3',
                                           '656839c5ee3bfa4b0f0de43c',
                                           '654c45ee62abc2d1ef76c9fe',
                                           '6559fbf63fdf813a88acc840',
                                           '656838b655a6228fd99bc3b9',
                                           '65b33fa2d6883afba4097f80',
                                           '654b37ee6d48f06c21072f4b',
                                           '6559fc44c58b5dc987a9ade5',
                                           '656838b65d9de92d89e030b1',
                                           '65b73dcf142f7d5a058bcc93',
                                           '65f25584c8ec11f8bf197125',
                                           '65f2d8d00057e9a84dfbbe0a',
                                           '65f3e5682543be46a06a7dcf',
                                           '65f2a6fc044d507567a7f55e',
                                           '65f258d12811e16db714a0f2',
                                           '65dbfc0ab5b9158a55a3a8a9',
                                           '656838afee7e56bd10e37335',
                                           '6548b03a99d99e3db867a1e9',
                                           '65f3e89d4a68c5f004b8edcf',
                                           '65eeedd838d883153b372b80',
                                           '6548b03af604306750455e3b',
                                           '65bf3f3f9087dee7e88403a6');