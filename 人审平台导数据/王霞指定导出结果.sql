-- 恺英
set @start='2024-10-20 00:00:00';
set @end='2024-10-25 23:59:59';
SET @start_time = UNIX_TIMESTAMP(@start)*1000;
SET @end_time = UNIX_TIMESTAMP(@end)*1000;
select source,task_id,trace_id,biz_type,scene_type,from_unixtime(round(time_stamp/1000),'%Y-%m-%d %H:%i:%s') as robot_time,role,content,risk_result,JSON_UNQUOTE(JSON_EXTRACT(report_json, '$.data.review_result')) AS review_result,valm_flag  from (
    select JSON_EXTRACT(JSON_UNQUOTE(JSON_EXTRACT(origin_data, '$.payload')),'$.source')  AS source,id,task_id,trace_id,biz_type,scene_type,time_stamp,role,content,risk_result,valm_flag
        from tbl_first_message_unreviewed where is_stream_stop=1 and biz_type in ('KaiYing_API','KaiYing') and scene_type!='npc' and time_stamp >=@start_time and time_stamp <=@end_time
    union all
    select JSON_EXTRACT(JSON_UNQUOTE(JSON_EXTRACT(origin_data, '$.payload')),'$.source')  AS source,id,task_id,trace_id,biz_type,scene_type,time_stamp,role,content,risk_result,valm_flag
        from tbl_first_message_reviewed where  is_stream_stop=1 and biz_type in ('KaiYing_API','KaiYing') and scene_type!='npc' and time_stamp >=@start_time and time_stamp <=@end_time
)a left join (select * from tbl_risk_report) b on a.id=b.message_id
order by time_stamp asc;


-- 万知监管账号--
set @start='2024-10-25 00:00:00';
set @end='2024-10-27 23:59:59';
SET @start_time = UNIX_TIMESTAMP(@start)*1000;
SET @end_time = UNIX_TIMESTAMP(@end)*1000;
select source,task_id,trace_id,biz_type,scene_type,from_unixtime(round(time_stamp/1000),'%Y-%m-%d %H:%i:%s') as robot_time,role,content,risk_result,JSON_UNQUOTE(JSON_EXTRACT(report_json, '$.data.review_result')) AS review_result,valm_flag  from (
    select JSON_EXTRACT(JSON_UNQUOTE(JSON_EXTRACT(origin_data, '$.payload')),'$.source')  AS source,id,task_id,trace_id,biz_type,scene_type,time_stamp,role,content,risk_result,valm_flag
        from tbl_first_message_unreviewed where is_stream_stop=1 and biz_type='wanzhi' and time_stamp >=@start_time and time_stamp <=@end_time and account_id in ('65479a9448c30ffa05dc8beb','65479a94dd3542cb9bb604cc','65479a947751412e4c9dd9f0','65479a9440dea251428e6787','65479a944cdd3a7031431e8e','65479a94390a068c78e7da9b','65479a9493cc5e9faacd2f75','65479a94accb0e199219122d','65479a947642096977f3c9b9','65479a944675fc8b04d33f7d','65479a947e97921e87ee008d','65479a947ffcc24c4f345442','65479a9465c918963dacd258','65479a94d77ac8a3b87c00b2','65479a94eb3199767c96a870','65479a940955e350f198386e','65479a949724300f84ce4848','65479a949d3d877f709d48d6','65479a94028733e2646cb678','65479a94663f16a1e18345e6','65479a94ec204e7320aa0d26','65479a9453c52801c81836f7','65479a9436bdb663c66a5d7e','65479a94597daa5c438bc778','65479a94d236d0251f8d45d1','65479a94af6873284c8194ad','65479a94ffc482132eb15400','65479a946225435ba6d3bc74','65479a94388a34082621c3fd','65479a947cb42857a396ba9e','65479a94d75705714b8ee814','65479a94b6f0c8f174f32d6d','65479a94c1df668285a5fa29','65479a94333cf30339c22d36','65479a945e9643739587d30f','65479a9428fab4d904066a68','65479a94566965027620c3d9','65479a94cc7fdd4af2ecf745','65479a945d1ef775c7e7c6f8','65479a94dcfaa1221d9b65c6','65479a94751dc085579f30a8','65479a94ae9bc76d6552a643','65479a942914623c24755a25','65479a949c8f537a4e1ca01f','65479a94c395bdb92ac1aa24','65479a943ea62c7024cdd813','65479a94e9f8fb724e544b39','65479a9409c7ccf61d144742','65479a9450c8d90ea5319d1d','65479a9427e76bcb10329d1f','65479a9489442779c34d17a6','65479a94d9be017f01c504d6','65479a940afaa2bfa67a7101','65479a94276111314584af1f','65479a94901837f2a58969c2')
    union all
    select JSON_EXTRACT(JSON_UNQUOTE(JSON_EXTRACT(origin_data, '$.payload')),'$.source')  AS source,id,task_id,trace_id,biz_type,scene_type,time_stamp,role,content,risk_result,valm_flag
        from tbl_first_message_reviewed where  is_stream_stop=1 and biz_type='wanzhi' and time_stamp >=@start_time and time_stamp <=@end_time and account_id in ('65479a9448c30ffa05dc8beb','65479a94dd3542cb9bb604cc','65479a947751412e4c9dd9f0','65479a9440dea251428e6787','65479a944cdd3a7031431e8e','65479a94390a068c78e7da9b','65479a9493cc5e9faacd2f75','65479a94accb0e199219122d','65479a947642096977f3c9b9','65479a944675fc8b04d33f7d','65479a947e97921e87ee008d','65479a947ffcc24c4f345442','65479a9465c918963dacd258','65479a94d77ac8a3b87c00b2','65479a94eb3199767c96a870','65479a940955e350f198386e','65479a949724300f84ce4848','65479a949d3d877f709d48d6','65479a94028733e2646cb678','65479a94663f16a1e18345e6','65479a94ec204e7320aa0d26','65479a9453c52801c81836f7','65479a9436bdb663c66a5d7e','65479a94597daa5c438bc778','65479a94d236d0251f8d45d1','65479a94af6873284c8194ad','65479a94ffc482132eb15400','65479a946225435ba6d3bc74','65479a94388a34082621c3fd','65479a947cb42857a396ba9e','65479a94d75705714b8ee814','65479a94b6f0c8f174f32d6d','65479a94c1df668285a5fa29','65479a94333cf30339c22d36','65479a945e9643739587d30f','65479a9428fab4d904066a68','65479a94566965027620c3d9','65479a94cc7fdd4af2ecf745','65479a945d1ef775c7e7c6f8','65479a94dcfaa1221d9b65c6','65479a94751dc085579f30a8','65479a94ae9bc76d6552a643','65479a942914623c24755a25','65479a949c8f537a4e1ca01f','65479a94c395bdb92ac1aa24','65479a943ea62c7024cdd813','65479a94e9f8fb724e544b39','65479a9409c7ccf61d144742','65479a9450c8d90ea5319d1d','65479a9427e76bcb10329d1f','65479a9489442779c34d17a6','65479a94d9be017f01c504d6','65479a940afaa2bfa67a7101','65479a94276111314584af1f','65479a94901837f2a58969c2')
)a left join (select * from tbl_risk_report) b on a.id=b.message_id
order by time_stamp asc
;

-- legacy --
set @start='2024-10-09 00:00:00';
set @end='2024-10-09 23:59:59';
SET @start_time = UNIX_TIMESTAMP(@start)*1000;
SET @end_time = UNIX_TIMESTAMP(@end)*1000;
select source,task_id,trace_id,biz_type,scene_type,from_unixtime(round(time_stamp/1000),'%Y-%m-%d %H:%i:%s') as robot_time,role,content,risk_result,JSON_UNQUOTE(JSON_EXTRACT(report_json, '$.data.review_result')) AS review_result,valm_flag  from (
    select JSON_EXTRACT(JSON_UNQUOTE(JSON_EXTRACT(origin_data, '$.payload')),'$.source')  AS source,id,task_id,trace_id,biz_type,scene_type,time_stamp,role,content,risk_result,valm_flag
        from tbl_first_message_unreviewed where is_stream_stop=1 and biz_type='legacy' and time_stamp >=@start_time and time_stamp <=@end_time
    union all
    select JSON_EXTRACT(JSON_UNQUOTE(JSON_EXTRACT(origin_data, '$.payload')),'$.source')  AS source,id,task_id,trace_id,biz_type,scene_type,time_stamp,role,content,risk_result,valm_flag
        from tbl_first_message_reviewed where  is_stream_stop=1 and biz_type='legacy' and time_stamp >=@start_time and time_stamp <=@end_time
)a left join (select * from tbl_risk_report) b on a.id=b.message_id
order by time_stamp asc;