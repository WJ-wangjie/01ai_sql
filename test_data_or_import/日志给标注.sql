-- 日志数据 去重后 只有raise4没有raise3
select *,((rist_result_raise3 = 'unsafe' or rist_result_raise4 = 'unsafe')) as isUnsafe
from (
WITH ranked_data AS (
    SELECT
        trace_id,
        timestamp,
        content,
        rist_result,
        event,
        ROW_NUMBER() OVER (PARTITION BY trace_id ORDER BY timestamp) AS min_row_num,
        ROW_NUMBER() OVER (PARTITION BY trace_id ORDER BY timestamp DESC) AS max_row_num
    FROM test_data_or_import.safe_data_log_20231117
    WHERE trace_id != 'baidu' and event in ('raise3','raise4')
)
SELECT
    trace_id,
    MAX(CASE WHEN event = 'raise3' THEN timestamp END) AS timestamp_raise3,
    MAX(CASE WHEN event = 'raise3' THEN content END) AS content_raise3,
    MAX(CASE WHEN event = 'raise3' THEN rist_result END) AS rist_result_raise3,
    MAX(CASE WHEN event = 'raise4' THEN timestamp END) AS timestamp_raise4,
    MAX(CASE WHEN event = 'raise4' THEN content END) AS content_raise4,
    MAX(CASE WHEN event = 'raise4' THEN rist_result END) AS rist_result_raise4
FROM ranked_data
WHERE (min_row_num = 1 OR max_row_num = 1)
GROUP BY trace_id
)aa -- where content_raise3 is not null and (rist_result_raise3 = 'unsafe' or rist_result_raise4 = 'unsafe')