select row_number() over () as num, person_data, shumei_data, htj_data, r3_data, r4_data, baidu_data, ali_data
from (
select
    prompt,data_type,
    max(CASE WHEN api_type = 'jq_r3' THEN risk_safe END) AS jq_r3_data,
    max(CASE WHEN api_type = 'jq_r4' THEN risk_safe END) AS jq_r4_data,
    max(CASE WHEN api_type = 'person' THEN risk_safe END) AS person_data,
    max(CASE WHEN api_type = 'shumei' THEN risk_safe END) AS shumei_data,
    max(CASE WHEN api_type = 'htj' THEN risk_safe END) AS htj_data,
    max(CASE WHEN api_type = 'raise3' THEN risk_safe END) AS r3_data,
    max(CASE WHEN api_type = 'raise4' THEN risk_safe END) AS r4_data,
    max(CASE WHEN api_type = 'baidu' THEN risk_safe END) AS baidu_data,
    max(CASE WHEN api_type = 'ali' THEN risk_safe END) AS ali_data
from (
         select distinct 'jq_r3' as api_type,content_raise3 as prompt,'input'as data_type,rist_result_raise3 as risk_safe
         from person_1117 where content_raise3 is not null and rist_result_raise3 ='unsafe'
         union all
         select distinct 'jq_r4' as api_type, content_raise4 as prompt,'output'as data_type,rist_result_raise4 as risk_safe
         from person_1117 where content_raise4 is not null  and rist_result_raise4 ='unsafe'
         union all
         select distinct 'person' as api_type,content_raise3 as prompt,'input'as data_type,r3审核员1 as risk_safe
         from person_1117 where content_raise3 is not null and rist_result_raise3 ='unsafe'
         union all
         select distinct 'person' as api_type,content_raise4 as prompt,'output'as data_type,r4审核员1 as risk_safe
         from person_1117 where content_raise4 is not null  and rist_result_raise4 ='unsafe'
         union all
         select api_type,prompt,data_type,risk_result as risk_safe
         from review_response_1117_end
     )tt  group by prompt,data_type)t;

select *
from review_request;