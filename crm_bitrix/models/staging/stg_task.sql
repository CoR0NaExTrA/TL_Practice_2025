{{
    config(
        materialized='view',
        alias='stg_task'
    )
}}

WITH cleared AS (
    SELECT
        CAST(id AS INT) AS product_id,
        CAST(parentid AS INT) AS parent_id,
        CAST(stageid AS INT) as stage_id,
        CAST(createdby AS INT) as created_by,
        CAST(responsibleid AS INT) as responsible_id,
        CAST(changedby AS INT) as changed_by,
        CAST(statuschangedby AS VARCHAR(255)) as status_changed_by,
        CAST(closedby AS INT) as closed_by,
        CAST(guid AS VARCHAR(255)) as guid,
        CAST(xmlid AS VARCHAR(255)) as xml_id,
        CAST(forkedbytemplate_id AS VARCHAR(255)) as forked_by_template_id,
        CAST(forumtopicid AS VARCHAR(255)) as forum_topic_id,
        CAST(forumid AS VARCHAR(255)) as forum_id,
        CAST(siteid AS VARCHAR(255)) as site_id,
        CAST(exchangeid AS VARCHAR(255)) as exchange_id,
        CAST(flowid AS VARCHAR(255)) as flow_id,
        CAST(uftasklocalportid AS VARCHAR(255)) as uf_task_local_port_id,
        CAST(groupid AS VARCHAR(255)) as group_id,

        CAST(title AS VARCHAR(255)) AS title,
        CAST(description AS VARCHAR(255)) as description,
        CAST(priority AS INT) as priority,
        CAST(commentscount AS INT) as comments_count,
        CAST(servicecommentscount AS INT) as service_comments_count,
        CAST(timeestimate AS INT) as time_estimate,
        CAST(timespent_in_logs AS INT) as timespent_in_logs,
        CAST(outlook_version AS INT) as outlook_version,
        CAST(durationfact AS INT) as duration_fact,
        CAST(status AS INT) as status,
        CAST(durationplan AS INT) as duration_plan,
        CAST(durationtype AS VARCHAR(255)) as duration_type,
        CAST(newcommentscount AS INT) as new_comments_count,
        CAST(substatus AS INT) as substatus,

        CASE 
            WHEN createddate ~ '^\d{4}-\d{2}-\d{2}' THEN createddate::TIMESTAMP
            ELSE NULL 
        END AS created_date,
        CASE 
            WHEN changeddate ~ '^\d{4}-\d{2}-\d{2}' THEN changeddate::TIMESTAMP
            ELSE NULL 
        END AS changed_date,
        CASE 
            WHEN closeddate ~ '^\d{4}-\d{2}-\d{2}' THEN closeddate::TIMESTAMP
            ELSE NULL 
        END AS closed_date,
        CASE 
            WHEN activitydate ~ '^\d{4}-\d{2}-\d{2}' THEN activitydate::TIMESTAMP
            ELSE NULL 
        END AS activity_date,
        CASE 
            WHEN datestart ~ '^\d{4}-\d{2}-\d{2}' THEN datestart::TIMESTAMP
            ELSE NULL 
        END AS date_start,
        CASE 
            WHEN deadline ~ '^\d{4}-\d{2}-\d{2}' THEN deadline::TIMESTAMP
            ELSE NULL 
        END AS deadline,
        CASE 
            WHEN startdateplan ~ '^\d{4}-\d{2}-\d{2}' THEN startdateplan::TIMESTAMP
            ELSE NULL 
        END AS start_date_plan,
        CASE 
            WHEN enddateplan ~ '^\d{4}-\d{2}-\d{2}' THEN enddateplan::TIMESTAMP
            ELSE NULL 
        END AS end_date_plan,
        CASE 
            WHEN vieweddate ~ '^\d{4}-\d{2}-\d{2}' THEN vieweddate::TIMESTAMP
            ELSE NULL 
        END AS viewed_date,
        CASE 
            WHEN statuschangeddate ~ '^\d{4}-\d{2}-\d{2}' THEN statuschangeddate::TIMESTAMP
            ELSE NULL 
        END AS status_changed_date,

        CASE 
            WHEN multitask = 'Y' OR multitask = '1' THEN 1
            WHEN multitask = 'N' OR multitask = '0' OR multitask = '' THEN 0
            ELSE NULL 
        END AS is_multitask,
        CASE 
            WHEN notviewed = 'Y' OR notviewed = '1' THEN 1
            WHEN notviewed = 'N' OR notviewed = '0' OR notviewed = '' THEN 0
            ELSE NULL 
        END AS is_notviewed,
        CASE 
            WHEN replicate = 'Y' OR replicate = '1' THEN 1
            WHEN replicate = 'N' OR replicate = '0' OR replicate = '' THEN 0
            ELSE NULL 
        END AS is_replicate,
        CASE 
            WHEN allowchangedeadline = 'Y' OR allowchangedeadline = '1' THEN 1
            WHEN allowchangedeadline = 'N' OR allowchangedeadline = '0' OR allowchangedeadline = '' THEN 0
            ELSE NULL 
        END AS allow_change_deadline,
        CASE 
            WHEN allowtimetracking = 'Y' OR allowtimetracking = '1' THEN 1
            WHEN allowtimetracking = 'N' OR allowtimetracking = '0' OR allowtimetracking = '' THEN 0
            ELSE NULL 
        END AS allow_time_tracking,
        CASE 
            WHEN taskcontrol = 'Y' OR taskcontrol = '1' THEN 1
            WHEN taskcontrol = 'N' OR taskcontrol = '0' OR taskcontrol = '' THEN 0
            ELSE NULL 
        END AS task_control,
        CASE 
            WHEN addinreport = 'Y' OR addinreport = '1' THEN 1
            WHEN addinreport = 'N' OR addinreport = '0' OR addinreport = '' THEN 0
            ELSE NULL 
        END AS add_in_report,
        CASE 
            WHEN matchworktime = 'Y' OR matchworktime = '1' THEN 1
            WHEN matchworktime = 'N' OR matchworktime = '0' OR matchworktime = '' THEN 0
            ELSE NULL 
        END AS match_worktime,
        CASE 
            WHEN subordinate = 'Y' OR subordinate = '1' THEN 1
            WHEN subordinate = 'N' OR subordinate = '0' OR subordinate = '' THEN 0
            ELSE NULL 
        END AS subordinate,
        CASE 
            WHEN ismuted = 'Y' OR ismuted = '1' THEN 1
            WHEN ismuted = 'N' OR ismuted = '0' OR ismuted = '' THEN 0
            ELSE NULL 
        END AS is_muted,
        CASE 
            WHEN ispinned = 'Y' OR ispinned = '1' THEN 1
            WHEN ispinned = 'N' OR ispinned = '0' OR ispinned = '' THEN 0
            ELSE NULL 
        END AS is_pinned,
        CASE 
            WHEN ispinnedingroup = 'Y' OR ispinnedingroup = '1' THEN 1
            WHEN ispinnedingroup = 'N' OR ispinnedingroup = '0' OR ispinnedingroup = '' THEN 0
            ELSE NULL 
        END AS is_pinned_in_group,
        CASE 
            WHEN descriptioninbbcode = 'Y' OR descriptioninbbcode = '1' THEN 1
            WHEN descriptioninbbcode = 'N' OR descriptioninbbcode = '0' OR descriptioninbbcode = '' THEN 0
            ELSE NULL 
        END AS description_in_bbcode,
        CASE 
            WHEN favorite = 'Y' OR favorite = '1' THEN 1
            WHEN favorite = 'N' OR favorite = '0' OR favorite = '' THEN 0
            ELSE NULL 
        END AS is_favorite
    FROM {{ source('landing', 'test_db.crm_process_product') }}
    WHERE id IS NOT NULL
)

SELECT * FROM cleared

"mark"
"exchangemodified"
"sorting"
"ufcrmtask"
"ufmailmessage"
"auditors"
"accomplices"
"group"
"creator"
"responsible"
"accomplicesdata"
"auditorsdata"
"tags"
"lead_ids"
"deal_ids"
"contact_ids"