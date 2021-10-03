import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2021,10,1),
    'end_date': datetime(2021,11,1),
    'email': ['thanhphuong.ftu2@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='bigquery_github_trends',
    default_args=default_args,
    schedule_interval = "00 21 * * *"
)
#Config variables
BQ_CONN_ID  = 'my_gcp_conn'
BQ_PROJECT = 'my-bq-project'
BQ_DATASET = 'my-bq-dataset'

t1 = BigQueryCheckOperator(
    task_id='bq_check_githubarchive_day',
    sql='''
    #standardSQL
    SELECT
    table_id
    FROM
    `githubarchive.day.__TABLES_SUMMARY__
    WHERE
    table_id = "{{ yesterday_ds_nodash }}"
    ''',
    use_legacy_sql=False,
    bigquery_conn_id=BQ_CONN_ID,
    dag=dag
)

t2 = BigQueryCheckOperator(
    task_id='bq_check_hackernews_full',
    sql='''
    #standardSQL
    SELECT
    FORMAT_TIMESTAMP("%Y%m%d", timestamp) AS date
    FROM
    `bigquery-public-data.hacker_news.full
    WHERE
    type='story'
    AND FORMAT_TIMESTAMP("%Y%m%d", timestamp) = "{{ yesterday_nodash }}
    LIMIT 1
    ''',
    use_legacy_sql=False,
    bigquery_conn_id=BQ_CONN_ID,
    dag=dag
)
#webserver airflow test [DAG_ID] [TASK_ID] [EXECUTION_DATE ]

t3 = BigQueryOperator(
    task_id='bq_write_to_github_daily_metrics',
    sql='''
    SELECT
    date,
    repo,
    SUM(IF(type='WatchEvent', 1, NULL)) AS starts,
    SUM(IF(type='ForkEvent', 1, NULL)) AS forks
    FROM (
    SELECT
    FORMAT_TIMESTAMP("%Y%m%d", created_at) AS date,
    actor.id as actor_id,
    repo.name as repo,
    type
    FROM
    `githubarchive.day.{{ yesterday_ds_nodash }}`
    WHHER type IN ('WatchEvent', 'ForkEvent')
    ''',
    destination_dataset_table='{0}.{1}.github_daily_metrics${2}'.format(
        BQ_PROJECT, BQ_DATASET, '{{ yesterday_ds_nodash }}'),
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    use_legacy_sql=False,
    bigquery_conn_id=BQ_CONN_ID,
    dag=dag
)

t4= BigQueryOperator(
    task_id='bq_write_to_github_agg',
    sql='''
    SELECT
    "{2}" as date,
    repo,
    SUM(stars) as stars_last_20_days,
    SUM(IF(_PARTITIONTIME BETWEEN TIMESTAMP("{4}") AND TIMESTAMP("{3}"), stars, null)) as stars_last_7_days,
    SUM(IF(_PARTITIONTIME BETWEEN TIMESTAMP("{3}") AND TIMESTAMP("{3}"), stars, null)) as stars_last_1_days,
    SUM(forks) as forks_last_20_days,
    SUM(IF(_PARTITIONTIME BETWEEN TIMESTAMP("{4}") AND TIMESTAMP("{3}"), forks, null)) as forks_last_7_days,
    SUM(IF(_PARTITIONTIME BETWEEN TIMESTAMP("{3}") AND TIMESTAMP("{3}"), forks, null)) as forks_last_1_days,
    FROM
    `{0}.{1}.github.daily_metrics`
    WHERE _PARTITIONTIME BETWEEN TIMESTAMP("{5}")
    AND TIMESTAMP("{5}")
    GROUP BY
    date,
    repo
    '''.format(BQ_PROJECT, BQ_DATASET, "{{ yesterday_ds_nodash }}", "{{ yesterday_ds }}",
    "{{macros.ds_add(ds, -6) }}", "{{ macros.ds_add(ds, -27) }}"),
    destination_dataset_table='{0}.{1}.github_agg${2}'.format(
        BQ_PROJECT, BQ_DATASET, '{{ yesterday_ds_nodash }}'
    ),
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    use_legacy_sql=False,
    bigquery_conn_id=BQ_CONN_ID,
    dag=dag
)

t5 = BigQueryOperator(
    task_id='bq_write_to_hackernews_agg',
    sql='''
    #standardSQL
    SELECT
    FROAMT_TIMESTAMP("%Y%m%d", timestamp) AS date,
    `by` AS submitter,
    id as story_id,
    REGEXP_EXTRACT(url, "(http?://github.com/[^/]*/[^/#?]*)") as url,
    SUM(score) as score
    FROM
    `bigquery-public-data.hacker_news.full`
    WHERE
    type='story'
    AND timestamp > '{{ yesterday_ds }}'
    AND timestamp < '{{ ds }}'
    AND url LIKE '%https://github.com%'
    AND url NOT LIKE '%github.com/blog/%'
    GROUP BY
    date,
    submitter,
    story_id,
    url
    ''',
    destination_dataset_table='{0}.{1}.hackernews_agg${2}'.format(
        BQ_PROJECT, BQ_DATASET, '{{ yesterday_ds_nodash }}'
    ),
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    use_legacy_sql=False,
    bigquery_conn_id=BQ_CONN_ID,
    dag=dag
),
t6 = BigQueryOperator(
    task_id='bq_write_to_hackernews_github.agg',
    sql='''
    SELECT
    a.date as date,
    a.url as github_url,
    b.repo as github_repo,
    a.score as hn_score,
    a.story_id as hn_story_id,
    b.stars_last_28_days as starts_last_28_days,
    b.stars_last_27_days as starts_last_7_days,
    b.stars_last_21_days as starts_last_1_days,
    b.forks_last_28_days as forks_last_28_days,
    b.forks_last_7_days as forks_last_7_days,
    b.forks_last_1_days as forks_last_1_days
    FROM
    (SELECT
    *
    FROM
    `{0}.{1}.hackernews_agg`
    WHERE _PARTITIONTIME BETWEEN TIMESTAMP("{2}") AND TIMESTAMP("{2}")) as a
    LEFT JOIN
    (
    SELECT,
    repo,
    CONCAT('https://github.com/', repo) as url,
    stars_last_28_days,
    stars_last_7_days,
    stars_last_1_days,
    forks_last_28_days,
    forks_last_7_days,
    forks_last_1_days
    FROM
    `{0}.{1}.github_agg`
    WHERE _PARTITIONTIME BETWEEN TIMESTAMP("{2}") AND TIMESTAMP("{2}")) AS b
    ON a.url = b.url
    '''.format( BQ_PROJECT, BQ_DATASET, "{{ yesterday_ds }}"),
    destination_dataset_table='{0}.{1}.hackernews_github_agg${2}'.format(
        BQ_PROJECT, BQ_DATASET, '{{ yesterday_ds_nodash}}'
    ),
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    use_legacy_sql=False,
    bigquery_conn_id=BQ_CONN_ID,
    dag=dag
)

t7= BigQueryCheckOperator(
    task_id='bq_check_hackernews_github_agg',
    sql='''
    SELECT
    COUNT(*) AS rows_in_partition
    FROM `{0}.{1}.hackernews_github_agg`
    WHERE _PARTITIONDATE = "{2}"
    '''.format(BQ_PROJECT, BQ_DATASET, '{{ yesterday_ds }}'),
    use_legacy_sql=False,
    bigquery_conn_id=BQ_CONN_ID,
    dag=dag
)

t3.set_upstream(t1)
t4.set_upstream(t3)
t5.set_upstream(t2)
t6.set_upstream(t4)
t6.set_upstream(t5)
t7.set_upstream(t6)