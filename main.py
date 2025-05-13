import dlt
from dlt.sources.helpers.rest_client.client import RESTClient
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator
from dlt.common.pipeline import ExtractInfo, NormalizeInfo, LoadInfo
from loguru import logger as l


def extract_runtime(info: ExtractInfo | NormalizeInfo | LoadInfo) -> float:
    return (
        info.asdict()["finished_at"] - 
        info.asdict()["started_at"]
    ).total_seconds()


@dlt.source()
def jaffle_shop():
    BASE_URL = "https://jaffle-shop.scalevector.ai/api/v1"
    
    client = RESTClient(
        BASE_URL,
        paginator=HeaderLinkPaginator()
    )

    @dlt.resource(table_name="customers")
    def customers():
        l.info("processing customers")
        for page in client.paginate("/customers"):
            for item in page:
                yield item

    @dlt.resource(table_name="products")
    def products():
        l.info("processing products")
        for page in client.paginate("/products"):  # limit data
            for item in page:
                yield item

    @dlt.resource(table_name="orders")
    def orders():
        l.info("processing orders")
        for page in client.paginate("/orders", params={"start_date": "2017-01-01", "end_date": "2017-01-07"}):
            l.debug("processing page")
            for item in page:
                yield item

    return customers, products, orders


pipeline = dlt.pipeline(
    pipeline_name="jaffle_shop_base",
    destination="duckdb",
    dataset_name="jaffle_shop",
)

extract_info = pipeline.extract(jaffle_shop())
l.success(f"Total extract time: {extract_runtime(extract_info)}")

normalize_info = pipeline.normalize()
l.success(f"Total normalize time: {extract_runtime(normalize_info)}")

load_info = pipeline.load()
l.success(f"Total load time: {extract_runtime(load_info)}")