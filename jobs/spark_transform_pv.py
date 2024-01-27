import sys
sys.path.append('/opt/airflow/dags/modules/') # hacky solution to import my ETL code

from pv_etl import PVTransform

transformer = PVTransform(staging_area={{}})