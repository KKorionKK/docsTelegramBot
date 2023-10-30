from pymongo import MongoClient
from bot.pipelineBuilder import PipelineBuilder
from bot.config import BaseConfig
from bot.resultFormatter import formatResult


class Database:
    def __init__(
        self,
        config=BaseConfig.get_config(),
        collection="database",
    ):
        self.client = MongoClient(
            config.get("CONNECTION_STRING"),
            username=config.get("USERNAME"),
            password=config.get("PASSWORD"),
            authSource=config.get("AUTH_SOURCE"),
        )
        self.db = self.client["admin"]
        self.col = self.db[collection]

        self.builder = PipelineBuilder()

    async def get_aggregation(self, group_type: str, _from: str, upto: str) -> dict:
        pipeline = self.builder.build_pipeline(
            group_type=group_type, start_date=_from, end_date=upto
        )
        if isinstance(pipeline, list):
            result = list(self.col.aggregate(pipeline=pipeline))
            formatted_result = {
                "dataset": [item["total_payment"] for item in result],
                "labels": [item["formatted_date"] for item in result],
            }
            new_result = formatResult(
                _from, upto, group_type=group_type, result=formatted_result
            )
            return new_result
        else:
            return pipeline
