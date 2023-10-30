from datetime import datetime, timedelta


def formatResult(start_date: str, end_date: str, group_type: str, result: dict):
    """
    Форматирует вывод, при надобности добавляет даты, в которые не было выплат
    """
    dt_from = datetime.fromisoformat(start_date)
    dt_upto = datetime.fromisoformat(end_date)
    if group_type == "day":
        current_date = dt_from
        all_days = {}
        while current_date <= dt_upto:
            date_string = (
                str(current_date).split(" ")[0] + "T" + str(current_date).split(" ")[1]
            )
            all_days[str(date_string)] = 0
            current_date += timedelta(days=1)

        labels: list = result.get("labels")
        dataset: list = result.get("dataset")
        temp = {labels[i]: dataset[i] for i in range(len(labels))}
        for key in all_days.keys():
            if temp.get(key):
                all_days[key] = temp.get(key)
        return {
            "dataset": [item for item in all_days.values()],
            "labels": [item for item in all_days.keys()],
        }
    if group_type == "hour":
        current_date = dt_from
        all_hours = {}
        while current_date <= dt_upto:
            date_string = (
                str(current_date).split(" ")[0] + "T" + str(current_date).split(" ")[1]
            )
            all_hours[str(date_string)] = 0
            current_date += timedelta(hours=1)

        labels: list = result.get("labels")
        dataset: list = result.get("dataset")
        temp = {labels[i]: dataset[i] for i in range(len(labels))}
        for key in all_hours.keys():
            if temp.get(key):
                all_hours[key] = temp.get(key)

        return {
            "dataset": [item for item in all_hours.values()],
            "labels": [item for item in all_hours.keys()],
        }
    else:
        return result
