import sys
sys.path.append('/opt/airflow/dags/modules/') # hacky solution to import my ETL code

import logging
from pv_etl import PVSparkTransform

transformer = PVSparkTransform(
    staging_area=str("{{ dag_run.conf }}"["staging_area"]),
    logger=logging.getLogger(__name__)
)

transformer.transform(int("{{ dag_run.conf }}"["ss_id"]))