with 

prep_package_version as

(
select 
    id,
    created,
    updated,
    package_version                                                            as quoting_package_version,
    correlation_uuid,
    rank() over (partition by correlation_uuid order by created, id desc) as package_rank

from {{ source('int_model_repo_raw', 'job_machining_quote') }}
)

select 
    id,
    created,
    updated,
    quoting_package_version,
    correlation_uuid

from prep_package_version
where package_rank = 1 