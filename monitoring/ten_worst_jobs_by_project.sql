CREATE OR REPLACE PROCEDURE
  -- REPLACE ${project_id} with your project_id
  `{project_id}`.monitoring_utils.ten_worst_jobs_by_project(project_id STRING,
    region STRING)
BEGIN
EXECUTE IMMEDIATE
  FORMAT( """
SELECT
  job_id,
  creation_time,
  user_email,
  statement_type,
  destination_table,
  referenced_tables,
  total_slot_ms,
  dml_statistics.inserted_row_count,
  dml_statistics.updated_row_count,
  dml_statistics.deleted_row_count
FROM
  `%s.%s`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE
  creation_time >= TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)
  AND parent_job_id IS NULL -- Only show top-level jobs
ORDER BY
  total_slot_ms desc
LIMIT
  10;
""", project_id, region);
END;
