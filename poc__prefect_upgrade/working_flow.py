from prefect import flow
from prefect.logging import get_run_logger

@flow
def my_flow(log_prints=True):
    logger = get_run_logger()
    logger.info("Hello from Prefect Flow!")
    logger.info("Bye from Prefect Flow!")

if __name__ == "__main__":
    my_flow.deploy(
        name="my-code-baked-into-an-image-deployment",
        work_pool_name="my-docker-pool",
        image="dockerized_prefect_flow",
        push=False
    )
