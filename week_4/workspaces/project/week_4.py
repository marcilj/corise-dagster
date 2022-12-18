from typing import List

from dagster import Nothing, String, asset, with_resources
from workspaces.resources import redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@asset(
    config_schema={"s3_key": String},
    required_resource_keys={"s3"},
    op_tags={"kind": "s3"},
    group_name="corise",
)
def get_s3_data(context) -> List[Stock]:
    files = context.resources.s3.get_data(context.op_config["s3_key"])
    return [Stock.from_list(file) for file in files]


@asset(
    group_name="corise",
)
def process_data(get_s3_data) -> Aggregation:
    max_stock = max(get_s3_data, key=lambda s: s.high)
    return Aggregation(date=max_stock.date, high=max_stock.high)


@asset(
    required_resource_keys={"redis"},
    op_tags={"kind": "redis"},
    group_name="corise",
)
def put_redis_data(context, process_data):
    context.resources.redis.put_data(name=process_data.date.strftime(r"%Y-%m-%d"), value=str(process_data.high))


@asset(
    required_resource_keys={"s3"},
    op_tags={"kind": "s3"},
    group_name="corise",
)
def put_s3_data(context, process_data):
    context.resources.s3.put_data("stocks/{date}/{high}".format(date = process_data.date, high = process_data.high), process_data)


get_s3_data_docker, process_data_docker, put_redis_data_docker, put_s3_data_docker = with_resources(
    definitions=[get_s3_data, process_data, put_redis_data, put_s3_data],
    resource_defs={"s3": s3_resource, "redis": redis_resource},
    resource_config_by_key={
        's3': {
            'config': {
                'bucket': 'dagster',
                'access_key': 'test',
                'secret_key': 'test',
                'endpoint_url': 'http://localhost:4566',
            }
        },
        'redis': {
            'config': {
                'host': 'redis',
                'port': 6379,
            }
        },
    },
)
