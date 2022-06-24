# import necessary packages ----
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG
from datetime import datetime, timedelta
import logging
import os
import sys

sys.path.append("/usr/local/airflow")
sys.path.append("/usr/local/airflow/dags/custom_operators")

from dags.report_utility import param
from custom_operators.postgres_transfer_operator import PostgresToSFTPTransferOperator

logging.info("import necessary constants ----")
DB_CONN_ID = "redshift_lake"

# set report date (i.e. run date)
REPORT_END_DATE = r"{{ ds }}"


REPORT_LOOKBACK_PERIOD = r"3 DAYS"
logging.info(
    f"The lookback period is exactly {REPORT_LOOKBACK_PERIOD} from the run date"
)

PLATFORM_column_name = 'enter_num_here'

# NOTE: use these creds to send report to both client and interal
SFTP_CONN_IDS = [
    "conn_id_1",
    "conn_id_2",
]

# NOTE: send report to both client and internal
SFTP_REPORT_DIRS = [
    "report_dir_1",
	"report_dir_2",
]

TEMP_DIRECTORY = param["path"] + "/path_name/"

# NOTE: run at 6:00 PM UTC every day (11:00 AM PST) CRON
SCHEDULE_INTERVAL = "0 18 * * *"

# set retry constraints
RETRIES = 1
RETRY_DELAY = timedelta(minutes=10)

logging.info("create dag ----")
dag = DAG(
    dag_id="dag_id_name",
    # NOTE: confirmed start date with Deena
    start_date=datetime(2022, 4, 7),
    default_args={"retries": RETRIES, "retry_delay": RETRY_DELAY},
    description="insert_JIRA_ticket_num_here",
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=False,
)

logging.info("create queries ----")
PLATFORM_TEXT_QUERY = r"""
WITH cte_sms AS (
	-- pull out custom fields that the client passed to us
	SELECT 
		JSON_EXTRACT_PATH_TEXT(database_name.table_name, 'singleline236') AS CampaignID
		, JSON_EXTRACT_PATH_TEXT(database_name.table_name, 'singleline237') AS ChannelID
		, JSON_EXTRACT_PATH_TEXT(database_name.table_name, 'singleline238') AS Group_Number
		, JSON_EXTRACT_PATH_TEXT(database_name.table_name,, 'singleline127') AS MemberID
		, JSON_EXTRACT_PATH_TEXT(database_name.table_name,, 'singleline128') AS SubscriberID
		, database_name.table_name.column_name AS FirstName
		, database_name.table_name.column_name  AS LastName
		, JSON_EXTRACT_PATH_TEXT(database_name.table_name, 'singleline3') AS Product
		, database_name.table_name.column_name AS Contact
		, COALESCE(database_name.table_name.value, 'English') AS RawLanguagePreference
		, database_name.table_name.column_name AS Status
		, TIMEZONE('US/Pacific', database_name.table_name.column_name) AS AttemptDateTime
		, RANK() OVER(
			PARTITION BY database_name.table_name.column_name 
			ORDER BY database_name.table_name.column_name DESC
			) AS contact_attempt_rank
	FROM 
		database_name.table_name 
	LEFT JOIN 
        database_name.table_name
        ON database_name.table_name.column_name = database_name.column_name
        AND database_name.table_name.column_name = database_name.table_name.column_name 
	LEFT JOIN 
        ON database_name.table_name.column_name = database_name.table_name.column_name
        AND database_name.table_name.column_name = database_name.table_name.column_name 
	LEFT JOIN 
        ON database_name.table_name.column_name = database_name.table_name.column_name
        AND database_name.table_name.column_name = database_name.table_name.column_name
	LEFT JOIN
	    database_name.table_name
		ON database_name.table_name.column_name = database_name.column_name
		AND database_name.table_name.standard_field_id = 23
    WHERE 
		database_name.table_name.column_name = {platform_account_id}
		AND database_name.table_name.last_name <> 'TEST'
        AND database_name.table_name.column_name IN (
            00000
            , 11111
            , 22222
            , 33333
            , 44444
            , 55555
            , 66666
            , 77777
            )
		AND TRUNC(DATE(database_name.table_name.created_on)) = TRUNC(DATE(DATE('{report_end_date}') - INTERVAL '{report_lookback_period}'))
    ORDER BY 
		, selectingfield
)
	-- NOTE: explicitly selecting fields here to drop the rank column
	selectingfield_1
	, selectingfield_2
	, selectingfield_3
	, selectingfield_4
	, selectingfield_5
	, selectingfield_6
	, selectingfield_7
	, selectingfield_8
	, selectingfield_9
	, CASE
		WHEN RawLanguagePreference = 'English' THEN 'ENG'
		WHEN RawLanguagePreference = 'Spanish' THEN 'SPA'
		ELSE RawLanguagePreference
		END AS LanguagePreference
	, selectingfield_10
	, selectingfield_11
FROM 
	table_name 
WHERE 
	select_field_to_sort_by = 1
;
"""

logging.info("inject queries with constants ----")
report_dict = {
    # NOTE: keys will be used in filename and client wants Camel Case
    "Email": PLATFORM_EMAIL_QUERY.format(
        platform_account_id=PLATFORM_ACCOUNT_ID,
        report_end_date=REPORT_END_DATE,
        report_lookback_period=REPORT_LOOKBACK_PERIOD,
    ),
    "Text": PLATFORM_TEXT_QUERY.format(
        platform_account_id=PLATFORM_ACCOUNT_ID,
        report_end_date=REPORT_END_DATE,
        report_lookback_period=REPORT_LOOKBACK_PERIOD,
    ),
}

logging.info("create start task ----")
start = DummyOperator(task_id="start", dag=dag)

logging.info("start generating a set of tasks for each report ---")
for report_name, query in report_dict.items():
	logging.info(f"Start building the {report_name} task...")

	logging.info("retrieve the execution date")
	execution_date_nodash = datetime.now().date().strftime('%Y%m%d')

	logging.info(f"dynamically create a filename for the {report_name} report")
	sftp_filename = f"re{report_name}_{execution_date_nodash}_messagedelivery.csv"
	logging.info(f"the SFTP filename for the {report_name} task is '{sftp_filename}'")
	
	logging.info("build a generate_and_send_report task")
	generate_and_send_report = PostgresToSFTPTransferOperator(
        dag=dag,
        task_id=f"generate_and_send_report_{report_name}",
        sql=query,
        # NOTE: this will be sending two different SFTP directories
        sftp_dir=SFTP_REPORT_DIRS,
        sftp_filename=sftp_filename,
        local_tmp_dir=TEMP_DIRECTORY,
        delete_tmp_file=True,
        create_tmp_path=True,
        # NOTE: this will be using two different SFTP creds
        sftp_conn_id=SFTP_CONN_IDS,
        postgres_conn_id=DB_CONN_ID,
        output_format="csv",
        include_headers=True,
        capitalize_headers=False,
        delimiter=",",
        provide_context=True,
    )
	logging.info(f"Finish building the {report_name} task...")
	
	logging.info("set order of tasks")
	# NOTE: see here for guide 
	#		https://airflow.apache.org/docs/apache-airflow/1.10.3/concepts.html?highlight=trigger#bitshift-composition
	start >> generate_and_send_report

logging.info("finish generating a set of tasks for each report ---")