from pv_pipeline import PVTransform


transformer = PVTransform(
    staging_area=str(dag_run_conf.get("staging_area")),
    logger=logging.getLogger(__name__)
)
transformer.transform(int(dag_run_conf.get("ss_id")))