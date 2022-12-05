from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, String, graph, op
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(
    config_schema={"s3_key": String},
    required_resource_keys={"s3"},
    tags={"kind": "s3"},
)
def get_s3_data(context):
    s3_key = context.op_config["s3_key"]
    data = context.resources.s3.get_data(s3_key)
    return [Stock.from_list(item) for item in data]


@op(
    ins={"stocks": In(dagster_type=List[Stock], description="A list of stocks")},
    out={"aggregation": Out(dagster_type=Aggregation, description="Aggregation of stock data")},
)
def process_data(context, stocks: List[Stock]):
    stock = max(stocks, key=lambda stock: stock.high)
    return Aggregation(date=stock.date, high=stock.high)


@op(
    required_resource_keys={"redis"},
    ins={"aggregation": In(dagster_type=Aggregation, description="Aggregation of stock data")},
    out=Out(dagster_type=Nothing),
    tags={"kind": "redis"},
)
def put_redis_data(context, aggregation: Aggregation):
    name = aggregation.date.isoformat()
    value = str(aggregation.high)
    context.resources.redis.put_data(name, value)


@op(
    required_resource_keys={"s3"},
    ins={"aggregation": In(dagster_type=Aggregation, description="Aggregation of stock data")},
    out=Out(dagster_type=Nothing),
    tags={"kind": "s3"},
)
def put_s3_data(context, aggregation: Aggregation):
    name = aggregation.date.isoformat()
    context.resources.s3.put_data(name, aggregation)


@graph
def week_2_pipeline():
    stocks = get_s3_data()
    results = process_data(stocks)
    put_s3_data(results)
    put_redis_data(results)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

week_2_pipeline_local = week_2_pipeline.to_job(
    name="week_2_pipeline_local",
    config=local,
    resource_defs={
        "s3": mock_s3_resource,
        "redis": ResourceDefinition.mock_resource(),
    },
)

week_2_pipeline_docker = week_2_pipeline.to_job(
    name="week_2_pipeline_docker",
    config=docker,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource,
    },
)
