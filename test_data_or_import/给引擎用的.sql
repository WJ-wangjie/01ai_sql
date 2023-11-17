-- 给引擎用的数据sql
select
    null as id,content as prompt, null as prompt_type,
    null as prompt_base,
    case event
        when 'raise3' then 'input'
        when 'raise4' then 'output'
        else 'aaaaa'
            end as data_type,
    1 as htj_need,
    1 as sm_need,
    1 as r4_need,
    1 as r4_need,
    1 as bd_need,
    1 as al_need,
    null as label,
    null as sheet,
    rist_result
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
    *
FROM ranked_data
WHERE (min_row_num = 1 OR max_row_num = 1)
)aa where rist_result ='unsafe'