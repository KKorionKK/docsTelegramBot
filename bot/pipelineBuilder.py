from datetime import datetime


class PipelineBuilder:
    def build_pipeline(
        self, group_type: str, start_date: str, end_date: str
    ) -> list | dict[str, str]:
        """
        Построит и вернет pipeline в виде списка словарей по введенным параметрам.

        Если group_type неверный, то вернет сообщение об ошибке.

        """
        if group_type == "month":
            return self.__build_by_month(
                datetime.fromisoformat(start_date), datetime.fromisoformat(end_date)
            )
        if group_type == "day":
            return self.__build_by_day(
                datetime.fromisoformat(start_date), datetime.fromisoformat(end_date)
            )
        if group_type == "hour":
            return self.__build_by_hour(
                datetime.fromisoformat(start_date), datetime.fromisoformat(end_date)
            )
        else:
            return {"message": "Invalid group type"}

    def __build_by_month(self, start_date: datetime, end_date: datetime) -> list:
        group = self.__add_group(["year", "month"])
        sort = self.__add_sort(["_id.month"])
        formatted_date = self.__format_date([])
        return [
            {"$match": {"dt": {"$gte": start_date, "$lte": end_date}}},
            group,
            sort,
            {
                "$project": {
                    "_id": 0,
                    "total_payment": "$total_payments",
                    "formatted_date": formatted_date,
                }
            },
        ]

    def __build_by_day(self, start_date: datetime, end_date: datetime) -> list:
        group = self.__add_group(["year", "month", "day"])
        sort = self.__add_sort(["_id.month", "_id.day"])
        formatted_date = self.__format_date(["day"])
        res = [
            {"$match": {"dt": {"$gte": start_date, "$lte": end_date}}},
            group,
            sort,
            {
                "$project": {
                    "_id": 0,
                    "total_payment": "$total_payments",
                    "formatted_date": formatted_date,
                }
            },
        ]
        return res

    def __build_by_hour(self, start_date: datetime, end_date: datetime) -> list:
        group = self.__add_group(["year", "month", "day", "hour"])
        sort = self.__add_sort(["_id.month", "_id.day", "_id.hour"])
        formatted_date = self.__format_date(["day", "hour"])
        return [
            {"$match": {"dt": {"$gte": start_date, "$lte": end_date}}},
            group,
            sort,
            {
                "$project": {
                    "_id": 0,
                    "total_payment": "$total_payments",
                    "formatted_date": formatted_date,
                }
            },
        ]

    def __add_group(self, groups: list):
        """
        Метод строит нужную группировку по выбранному типу группировки.

        Пример groups = ["year", "month"] - добавит соответствующие значения

        Допустимые значения - "year", "month", "day", "hour"
        """
        _patterns = {
            "year": {"$year": "$dt"},
            "month": {"$month": "$dt"},
            "day": {"$dayOfMonth": "$dt"},
            "hour": {"$hour": "$dt"},
        }
        group = {key: _patterns.get(key) for key in groups if key in _patterns}
        return {
            "$group": {"_id": group, "total_payments": {"$sum": "$value"}},
        }

    def __add_sort(self, sorts: list):
        """Метод строит сортировку в зависимости от типа группировки

        Пример sorts = ["_id.month", "_id.day"] - применит
        сортировку по заданным параметрам

        Допустимые значения - "_id.month", "_id.day", "_id.hour"
        """
        _patterns = {
            "_id.day": 1,
            "_id.hour": 1,
            "_id.month": 1,
        }
        sort = {key: _patterns.get(key) for key in sorts if key in _patterns}
        return {
            "$sort": sort,
        }

    def __format_date(self, format_groups: list):
        """Метод возвращает правила форматирования
        даты в зависимости от типа группировки

        Пример format_groups = ["day", "hour"]

        Допустимые значения - "day", "hour"
        """
        day = "$_id.day" if "day" in format_groups else 1
        hour = "$_id.hour" if "hour" in format_groups else 0
        return {
            "$dateToString": {
                "format": "%Y-%m-%dT%H:00:00",
                "date": {
                    "$dateFromParts": {
                        "year": "$_id.year",
                        "month": "$_id.month",
                        "day": day,
                        "hour": hour,
                        "minute": 0,
                        "second": 0,
                    }
                },
            }
        }
