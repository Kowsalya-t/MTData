CREATE OR REPLACE VIEW inactive_sessions as (
    select a.process, 
    trim(a.user_name) as user_name,
    trim(c.remotehost) as remotehost,
    a.usesysid, 
    a.starttime, 
    datediff(s,a.starttime,sysdate) as session_dur, 
    b.last_end, 
    datediff(s,case when b.last_end is not null then b.last_end else a.starttime end,sysdate) idle_dur
        FROM
        (
            select starttime,process,u.usesysid,user_name 
            from stv_sessions s, pg_user u 
            where 
            s.user_name = u.usename 
            and u.usesysid>1
            and process NOT IN (select pid from stv_inflight where userid>1 
            union select pid from stv_recents where status != 'Done' and userid>1)
        ) a 
        LEFT OUTER JOIN (
            select 
            userid,pid,max(endtime) as last_end from svl_statementtext 
            where userid>1 and sequence=0 group by 1,2) b ON a.usesysid = b.userid AND a.process = b.pid

        LEFT OUTER JOIN (
            select username, pid, remotehost from stl_connection_log
            where event = 'initiating session' and username <> 'rsdb') c on a.user_name = c.username AND a.process = c.pid
        WHERE (b.last_end > a.starttime OR b.last_end is null)
        ORDER BY idle_dur
);

