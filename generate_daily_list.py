import datetime
import sys

from redis import Redis
from rq import Queue

import combined_lists
from shared import DATE_FORMAT_WITH_HYPHEN, DEFAULT_TRANCO_CONFIG



def get_date_interval_bounds(start_date, end_date, nb_days, nb_days_from):
    if start_date:
        start_date_dt = datetime.datetime.strptime(start_date, DATE_FORMAT_WITH_HYPHEN)
        return (
        start_date, (start_date_dt + datetime.timedelta(days=int(nb_days) - 1)).strftime(DATE_FORMAT_WITH_HYPHEN))
    elif end_date:
        end_date_dt = datetime.datetime.strptime(end_date, DATE_FORMAT_WITH_HYPHEN)
        return ((end_date_dt - datetime.timedelta(days=int(nb_days) - 1)).strftime(DATE_FORMAT_WITH_HYPHEN), end_date)


def generate_todays_lists(day):
    print("Generating lists for {}...".format(day))
    config = DEFAULT_TRANCO_CONFIG.copy()

    if day == "yesterday":
        date = (datetime.datetime.utcnow() - datetime.timedelta(days=1)).strftime(DATE_FORMAT_WITH_HYPHEN)
    elif day == "today":
        date = datetime.datetime.utcnow().strftime(DATE_FORMAT_WITH_HYPHEN)
    else:
        raise ValueError
    config["startDate"], config["endDate"] = get_date_interval_bounds(None, date, 30, "end")
    config["isDailyList"] = True

    print("Generating list...")
    list_id = combined_lists.config_to_list_id(config)
    print("Generating list ID {}...".format(list_id))
    if not combined_lists.list_available(list_id):
        conn = Redis('localhost', 6379)
        generate_queue = Queue('generate', connection=conn, default_timeout="1h")
        if list_id not in generate_queue.job_ids:
            generate_queue.enqueue(combined_lists.generate_combined_list, args=(config, list_id), job_id=str(list_id), timeout="1h")
            print("Submitted job for list ID {}".format(list_id))


if __name__ == '__main__':
    day = "yesterday"
    if len(sys.argv) > 1:
        day = sys.argv[1]
    generate_todays_lists(day)
