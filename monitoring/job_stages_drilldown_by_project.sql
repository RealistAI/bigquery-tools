CREATE OR REPLACE PROCEDURE
  -- REPLACE ${project_id} with your project_id
  `${project_id}`.monitoring_utils.job_stages_drilldown_by_project(project_id STRING,
    region STRING,
    job_id STRING)
BEGIN
EXECUTE IMMEDIATE
  FORMAT( """
SELECT
  query,
  MAX(total_slot_ms) AS total_slot_ms,
  ARRAY_AGG(STRUCT(stg.name AS name,
      stg.slot_ms AS slot_ms,
      stg.records_read as records_read,
      stg.records_written as records_written,
      (
      SELECT
        STRING_AGG(kind, '\\n')
      FROM
        UNNEST(stg.steps)) AS steps)) AS steps
FROM
  `%s.%s`.INFORMATION_SCHEMA.JOBS,
  UNNEST(job_stages) AS stg
WHERE
  COALESCE(parent_job_id, job_id) = '%s'
GROUP BY
  query;
""", project_id, region, job_id);
END
  ;
