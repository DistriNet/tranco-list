# Imports
import csv
import datetime
import glob
import shutil
import time
import traceback
import zipfile
from itertools import islice
import os
import tempfile

# Imports of configuration variables
from global_config import *

# Constants
GLOBAL_MAX_RANK = 1000000
LIST_FILENAME_FORMAT = "{}.csv"
from shared import ZIP_FILENAME_FORMAT

# When using AWS services, set up retrieval and storage of lists for S3
if USE_S3:
    import boto3
    s3_resource = boto3.resource('s3', region_name="us-east-1")
    toplists_archive_bucket = s3_resource.Bucket(name=TOPLISTS_ARCHIVE_S3_BUCKET)
    from smart_open import smart_open

# List ID generation
from hashids import Hashids
hsh = Hashids(salt="tsr", min_length=4, alphabet="BCDFGHJKLMNPQRSTVWXYZ23456789")

# Mongo connection for storing configuration of generated lists
from pymongo import MongoClient
client = MongoClient(MONGO_URL)
db = client["tranco"]

def count_dict(dct, entry, value=1):
    """ Helper function for updating dictionaries """
    if not entry in dct:
        dct[entry] = 0
    dct[entry] += value

def date_list(start_date, end_date):
    """ Generate list of dates between start and end date """
    start_date_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_date_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    return [(start_date_dt + datetime.timedelta(days=x)) for x in range((end_date_dt - start_date_dt).days + 1)]

def _db_id_to_list_id(db_id):
    """ List number to hash """
    if db_id:
        return hsh.encode(db_id)
    else:
        return None

def _list_id_to_db_id(list_id):
    """ Hash to list number """
    try:
        return hsh.decode(list_id)[0]
    except:
        return None

def config_to_list_id(config, insert=True, skip_failed=False):
    """ List configuration to list hash (either insert new configuration into database, or retrieve ID for existing list with that configuration)
    :param config: list configuration
    :param insert: whether to create a new list ID if the given configuration does not exist yet
    :param skip_failed: skip failed lists
    :return:
    """

    if skip_failed:
        query = {**config, "failed": {"$ne": True}}
    else:
        query = config
    out = db["lists"].find_one(query)
    if out:
        db_id = int(out["_id"])
    else:
        if insert:
            db_id = get_next_db_key()
            insert_config_in_db(config, db_id)
        else:
            return None
    return _db_id_to_list_id(db_id)

def list_id_to_config(list_id):
    """ Retrieve configuration of existing list based on hash """
    db_id = _list_id_to_db_id(list_id)
    if db_id:
        return {**db["lists"].find_one({"_id": int(db_id)}), "list_id": list_id}

def list_available(list_id):
    """ Check if list is available for download """
    db_id = _list_id_to_db_id(list_id)
    if not db_id:
        return False
    doc = db["lists"].find_one({"_id": int(db_id)})
    return doc is not None and doc.get("finished", False) and not doc.get("failed", True)

def get_next_db_key():
    """ Get next key from list configuration database (for a new list) """
    counter_increase = db["counter"].find_one_and_update({"_id": "lists"}, {'$inc': {'count': 1}})
    return int(counter_increase["count"])

def insert_config_in_db(config, db_id):
    """ Insert a new configuration into the database, with the given key """
    db["lists"].insert_one({**config, "_id": db_id, "finished": False,
                            "creationDate": datetime.datetime.now().strftime("%Y-%m-%d"),
                            "creationTime": datetime.datetime.now().isoformat()})

def get_generated_list_fp(list_id):
    """ Get file location of existing list (file-based archive) """
    return os.path.join(NETAPP_STORAGE_PATH, "generated_lists/{}".format(LIST_FILENAME_FORMAT.format(list_id)))

def get_generated_zip_fp(list_id):
    """ Get file location of existing zip (file-based archive) """
    return os.path.join(NETAPP_STORAGE_PATH, "generated_lists_zip/{}".format(ZIP_FILENAME_FORMAT.format(list_id)))

def get_generated_list_s3(list_id):
    """ Get file location of existing list (AWS S3) """
    return "s3://{}/{}".format(TOPLISTS_GENERATED_LIST_S3_BUCKET, LIST_FILENAME_FORMAT.format(list_id))

def get_generated_zip_s3(list_id):
    """ Get file location of existing zip (AWS S3) """
    return "s3://{}/{}".format(TOPLISTS_DAILY_LIST_S3_BUCKET, ZIP_FILENAME_FORMAT.format(list_id))

def get_list_fp_for_day(provider, date, parts=False):
    """ Get file location for source list (of one of the providers) """
    date = date.strftime("%Y%m%d")
    if parts:
        fp = next(glob.iglob(os.path.join(NETAPP_STORAGE_PATH, "archive/{}/parts/{}_{}_parts.csv".format(provider, provider, date))))
    else:
        fp = next(glob.iglob(os.path.join(NETAPP_STORAGE_PATH, "archive/{}/{}_{}.csv".format(provider, provider, date))))
    return fp

def get_s3_key_for_day(provider, date, parts=False):
    """ Get S3 key for source list (of one of the providers) """
    date = date.strftime("%Y%m%d")
    if parts:
        fp = "{}/parts/{}_{}_parts.csv".format(provider, provider, date)
    else:
        fp = "{}/{}_{}.csv".format(provider, provider, date)
    return fp

def get_s3_url_for_day(provider, date, parts=False):
    """ Get S3 url for source list (of one of the providers) """
    key = get_s3_key_for_day(provider, date, parts)
    return "s3://{}/{}".format(TOPLISTS_ARCHIVE_S3_BUCKET, key)

def get_s3_url_for_fp(fp):
    """ Get S3 url for source list (of one of the providers) """
    return "s3://{}/{}".format(TOPLISTS_ARCHIVE_S3_BUCKET, fp)

def generate_prefix_items_file(fp, list_prefix):
    """ Create list of source list items (up to requested list length) """
    with open(fp, encoding='utf8') as f:
        if list_prefix:
            return [r.split(",") for r in islice(f.read().splitlines(), list_prefix)]
        else:
            return [r.split(",") for r in f.read().splitlines()]

def generate_prefix_items_s3(fp, list_prefix):
    """ Create list of source list items (up to requested list length) """
    with smart_open(get_s3_url_for_fp(fp)) as f:
        if list_prefix:
            result = [r.decode("utf-8").split(",") for r in islice(f.read().splitlines(), list_prefix)]
        else:
            result = [r.decode("utf-8").split(",") for r in f.read().splitlines()]
        return result

def rescale_rank(rank, max_rank_of_input, min_rank_of_output, max_rank_of_output):
    """
    Rescale a given rank to the min/max range provided
    This makes sure that shorter lists are not given a higher importance.
    """
    return min_rank_of_output + (rank - 1)*((max_rank_of_output-min_rank_of_output)/(max_rank_of_input - 1))

def borda_count_fp(fps, list_prefix):
    """ Generate aggregate scores for domains based on Borda count """
    borda_scores = {}
    for fp in fps:
        if USE_S3:
            items = generate_prefix_items_s3(fp, list_prefix)
        else:
            items = generate_prefix_items_file(fp, list_prefix)
        max_rank_of_input = len(items)
        max_rank_of_output = min(GLOBAL_MAX_RANK, list_prefix if list_prefix else GLOBAL_MAX_RANK)
        for rank, elem in items:
            count_dict(borda_scores, elem, max_rank_of_output + 1 - rescale_rank(int(rank), max_rank_of_input, 1, max_rank_of_output))  # necessary to rescale shorter lists (i.e. Quantcast)
    return borda_scores

def dowdall_count_fp(fps, list_prefix):
    """ Generate aggregate scores for domains based on Dowdall count """
    dowdall_scores = {}
    for fp in fps:
        if USE_S3:
            items = generate_prefix_items_s3(fp, list_prefix)
        else:
            items = generate_prefix_items_file(fp, list_prefix)
        max_rank_of_input = len(items)
        max_rank_of_output = min(GLOBAL_MAX_RANK, list_prefix if list_prefix else GLOBAL_MAX_RANK)
        for rank, elem in items:
            count_dict(dowdall_scores, elem, 1 / rescale_rank(int(rank), max_rank_of_input, 1, max_rank_of_output))  # necessary to rescale shorter lists (i.e. Quantcast)
    return dowdall_scores

def filtered_parts_list_file(fp, list_prefix, f_pld=None, f_tlds=None, f_organization=None, f_subdomains=None, maintain_rank=True):
    """ Get list of domains that conform to the set filters """
    with open(fp) as f:
        if list_prefix:
            parts_input = islice(f, list_prefix)
        else:
            parts_input = f
        output = []
        organizations_seen = set()
        new_rank = 1
        max_rank = 0
        for line in parts_input:
            max_rank += 1
            rank, fqdn, pld, sld, subd, ps, tld, is_pld = line.rstrip().split(",")
            if f_tlds and (tld not in f_tlds):
                continue
            if f_subdomains and (subd not in f_subdomains):
                continue
            if f_organization:
                if sld in organizations_seen:
                    continue
                else:
                    organizations_seen.add(sld)
            if f_pld:
                if is_pld != "True":
                    continue
            if maintain_rank:
                output.append((rank, fqdn))
            else:
                output.append((new_rank, fqdn))
                new_rank += 1
    return (output, max_rank)

def filtered_parts_list_s3(fp, list_prefix, f_pld=None, f_tlds=None, f_organization=None, f_subdomains=None, maintain_rank=True):
    """ Get list of domains that conform to the set filters """
    with smart_open(get_s3_url_for_fp(fp)) as f:
        if list_prefix:
            parts_input = islice(f, list_prefix)
        else:
            parts_input = f
        output = []
        organizations_seen = set()
        new_rank = 1
        max_rank = 0
        for line in parts_input:
            max_rank += 1
            rank, fqdn, pld, sld, subd, ps, tld, is_pld = line.decode("utf-8").rstrip().split(",")
            if f_tlds and (tld not in f_tlds):
                continue
            if f_subdomains and (subd not in f_subdomains):
                continue
            if f_organization:
                if sld in organizations_seen:
                    continue
                else:
                    organizations_seen.add(sld)
            if f_pld:
                if is_pld != "True":
                    continue
            if maintain_rank:
                output.append((rank, fqdn))
            else:
                output.append((new_rank, fqdn))
                new_rank += 1
    return (output, max_rank)

def get_filtered_parts_lists(fps, input_prefix, config, maintain_rank=True):
    """ Get domains in given source lists that conform to the filters in the configuration """
    for fp in fps:
        if USE_S3:
            yield filtered_parts_list_s3(fp, input_prefix,
                                          config.get("filterPLD", None) == "on",
                                          config.get('filterTLDValue').split(",") if config.get("filterTLDValue",
                                                                                                None) else None,
                                          config.get("filterOrganization", None) == "on",
                                          config.get('filterSubdomainValue').split(",") if config.get(
                                              "filterSubdomainValue", None) else None,
                                         maintain_rank=maintain_rank
                                          )
        else:
            yield filtered_parts_list_file(fp, input_prefix,
                                         config.get("filterPLD", None) == "on",
                                         config.get('filterTLDValue').split(",") if config.get("filterTLDValue",
                                                                                               None) else None,
                                         config.get("filterOrganization", None) == "on",
                                         config.get('filterSubdomainValue').split(",") if config.get(
                                             "filterSubdomainValue", None) else None,
                                           maintain_rank=maintain_rank
                                         )

def borda_count_list(fps, input_prefix, config, maintain_rank=True):
    """ Generate aggregate scores for list of filtered domains based on Borda count """
    borda_scores = {}
    for (filtered_lst, max_rank) in get_filtered_parts_lists(fps, input_prefix, config):
        if maintain_rank:
            max_rank_of_input = max_rank
        else:
            max_rank_of_input = len(filtered_lst)
        max_rank_of_output = min(GLOBAL_MAX_RANK, input_prefix if input_prefix else GLOBAL_MAX_RANK)
        for rank, elem in filtered_lst:
            count_dict(borda_scores, elem, max_rank_of_output + 1 - rescale_rank(int(rank), max_rank_of_input, 1, max_rank_of_output))  # necessary to rescale shorter lists
    return borda_scores

def dowdall_count_list(fps, input_prefix, config, maintain_rank=True):
    """ Generate aggregate scores for list of filtered domains based on Dowdall count """
    dowdall_scores = {}
    for (filtered_lst, max_rank) in get_filtered_parts_lists(fps, input_prefix, config):
        if maintain_rank:
            max_rank_of_input = max_rank
        else:
            max_rank_of_input = len(filtered_lst)
        max_rank_of_output = min(GLOBAL_MAX_RANK, input_prefix if input_prefix else GLOBAL_MAX_RANK)
        for rank, elem in filtered_lst:
            count_dict(dowdall_scores, elem, 1 / rescale_rank(int(rank), max_rank_of_input, 1, max_rank_of_output))  # necessary to rescale shorter lists
    return dowdall_scores

def sort_counts(scores):
    """ Sort domains based on aggregate scores """
    return sorted(scores.keys(), key=lambda elem: (-scores[elem], elem))

def filter_list_1(lst, filter_set, list_size=None):
    """ Filter list of domains on given set of domains """
    if list_size:
        result = []
        for e in lst:
            if e in filter_set:
                result.append(e)
                if len(result) >= list_size:
                    break
        return result
    else:
        return [e for e in lst if e in filter_set]

def filter_list_multiple(lst, filter_sets):
    """ Filter list of domains on given sets of domains """
    return [e for e in lst if all(e in filter_set for filter_set in filter_sets)]

def count_presence_in_fps(fps, prefix):
    """ Counts of occurrences in given files with domains """
    presence = {}
    for fp in fps:
        lst = generate_prefix_items_s3(fp, prefix)
        for i in lst:
            count_dict(presence, i, 1)

def count_presence_in_sets(sets,):
    """ Counts of occurrences in given sets """
    presence = {}
    for st in sets:
        for i in st:
            count_dict(presence, i, 1)
    return presence

def items_in_any_list(fps, prefix):
    """ Find domains that appear in any of the given lists """
    return set.union(*map(set, [[i[1] for i in generate_prefix_items_s3(fp, prefix)] for fp in fps]))

def generate_filter_minimum_presence(fps, prefix, minimum):
    """ An item should appear on all the lists """
    presence = count_presence_in_fps(fps, prefix)
    return {k for k, v in presence.items() if v >= minimum}

def generate_filter_minimum_presence_any(groups_of_fps, prefix, minimum):
    """ An item should appear in `minimum` groups, where an item may appear in any list in that group """
    items_per_group = [items_in_any_list(group, prefix) for group in groups_of_fps]
    presence = count_presence_in_sets(items_per_group,)
    return {k for k, v in presence.items() if v >= minimum}

def truncate_list(lst, list_size=None):
    """ Return only prefix of given list """
    return lst[:list_size] if list_size else lst

def write_sorted_counts(sorted_items, scores, fp):
    """ Write domains and aggregate scores to file """
    with open(fp, 'w', encoding='utf8') as f:
        csvw = csv.writer(f)
        for idx, entry in enumerate(sorted_items):
            csvw.writerow([idx + 1, entry, scores[entry]])

def write_list_to_file(lst, list_id):
    """ Write ranks and domains to file """
    with open(get_generated_list_fp(list_id), 'w', encoding='utf8') as f:
        csvw = csv.writer(f)
        for idx, entry in enumerate(lst):
            csvw.writerow([idx + 1, entry])


def write_zip_to_file(lst, list_id):
    """ Write list of (top 1M) domains to zip file """
    with tempfile.SpooledTemporaryFile(mode='w+b') as z:
        with tempfile.NamedTemporaryFile(mode='w+') as t:
            csvw = csv.writer(t)
            for idx, entry in enumerate(lst):
                csvw.writerow([idx + 1, entry])

            t.seek(0)

            with zipfile.ZipFile(z, 'w') as a:
                a.write(t.name, arcname="top-1m.csv")

            z.seek(0)

            with open(get_generated_zip_fp(list_id), 'wb') as f:
                f.write(z.read())


def write_list_to_s3(lst, list_id):
    """ Write ranks and domains to file """
    with smart_open(get_generated_list_s3(list_id), 'w', encoding='utf8') as f:
        csvw = csv.writer(f)
        for idx, entry in enumerate(lst):
            csvw.writerow([idx + 1, entry])


def write_zip_to_s3(lst, list_id):
    """ Write list of (top 1M) domains to zip file """
    with tempfile.SpooledTemporaryFile(mode='w+b') as z:
        with tempfile.NamedTemporaryFile(mode='w+') as t:
            csvw = csv.writer(t)
            for idx, entry in enumerate(lst):
                csvw.writerow([idx + 1, entry])

            t.seek(0)

            with zipfile.ZipFile(z, 'w') as a:
                a.write(t.name, arcname="top-1m.csv")

            z.seek(0)

            with smart_open(get_generated_zip_s3(list_id), 'wb') as f:
                f.write(z.read())


def copy_daily_list_s3(list_id):
    """ Copy the daily list on S3 to the fixed URL """
    zip_key = ZIP_FILENAME_FORMAT.format(list_id)
    source = {'Bucket': TOPLISTS_DAILY_LIST_S3_BUCKET, 'Key': zip_key}
    target_bucket = s3_resource.Bucket(TOPLISTS_DAILY_LIST_S3_BUCKET)
    target_bucket.copy(source, 'top-1m.csv.zip')


def copy_daily_list_file(list_id):
    """ Copy the daily list on file-based archive to the fixed URL """
    zip_file = get_generated_zip_fp(list_id)
    target_file = os.path.join(NETAPP_STORAGE_PATH, "generated_lists_zip/{}".format("top-1m.csv.zip"))
    shutil.copy2(zip_file, target_file)

def generate_combined_list(config, list_id, test=False):
    """ Generate combined list by calculating aggregate scores on (potentially filtered) source lists of ranked domains """
    db_id = _list_id_to_db_id(list_id)
    try:
        ### INPUT ###

        # If a filter on parts is selected, the preprocessed parts files should be used.
        parts_filter = config.get("filterPLD", False) or (config.get("filterTLD", "false") != "false") or config.get("filterOrganization", False) or config.get('filterSubdomain', False)
        dates = date_list(config.get("startDate"), config.get("endDate"))

        # Get source files to process
        fps = []
        fps_on_date = {date: [] for date in dates}
        fps_on_provider = {provider: [] for provider in config['providers']}
        for provider in config['providers']:
            for date in dates:
                if USE_S3:
                    list_fp = get_s3_key_for_day(provider, date, parts_filter)
                else:
                    list_fp = get_list_fp_for_day(provider, date, parts_filter)
                fps.append(list_fp)
                fps_on_date[date].append(list_fp)
                fps_on_provider[provider].append(list_fp)

        # Get requested list prefix
        if "listPrefix" in config and config['listPrefix']:
            if config['listPrefix'] == "full":
                input_prefix = None
            elif config['listPrefix'] == "custom":
                input_prefix = int(config['listPrefixCustomValue'])
            else:
                input_prefix = int(config['listPrefix'])
        else:
            input_prefix = None

        # Generate (sorted) aggregate counts (on parts files if necessary)
        if parts_filter:
            if config['combinationMethod'] == 'borda':
                scores = borda_count_list(fps, input_prefix, config)
            elif config['combinationMethod'] == 'dowdall':
                scores = dowdall_count_list(fps, input_prefix, config)
            else:
                raise Exception("Unknown combination method")
        else:
            if config['combinationMethod'] == 'borda':
                scores = borda_count_fp(fps, input_prefix)
            elif config['combinationMethod'] == 'dowdall':
                scores = dowdall_count_fp(fps, input_prefix)
            else:
                raise Exception("Unknown combination method")
        sorted_domains = sort_counts(scores)
        domains = sorted_domains

        ### FILTERS ###

        filters_to_apply = []
        if "inclusionDays" in config and config["inclusionDays"]:
            presence_filter = generate_filter_minimum_presence_any([fps_on_date[date] for date in dates], input_prefix, int(config["inclusionDaysValue"]))
            filters_to_apply.append(presence_filter)
        if "inclusionLists" in config and config["inclusionLists"]:
            presence_filter = generate_filter_minimum_presence_any([fps_on_provider[provider] for provider in config['providers']], input_prefix, int(config["inclusionListsValue"]))
            filters_to_apply.append(presence_filter)
        domains = filter_list_multiple(domains, filters_to_apply)

        ### OUTPUT ###

        if test:
            return domains
        else:
            # Write list to file
            if USE_S3:
                write_list_to_s3(domains, list_id)
            else:
                write_list_to_file(domains, list_id)

            # If the list is the daily default list, also generate a zip of the top 1M and copy to permanent URL
            try:
                if "isDailyList" in config and config["isDailyList"] is True:
                    if USE_S3:
                        write_zip_to_s3(domains[:1000000], list_id)
                        copy_daily_list_s3(list_id)
                    else:
                        write_zip_to_file(domains[:1000000], list_id)
                        copy_daily_list_file(list_id)
            except:
                print("Zip creation failed")
                traceback.print_exc()

            # Update generation success in database
            db["lists"].update_one({"_id": db_id}, {"$set": {"finished": True, "failed": False, "list_id": list_id}})

        time.sleep(1)
        # Report success
        return True
    except:
        traceback.print_exc()
        # Update generation failure in database
        db["lists"].update_one({"_id": db_id}, {"$set": {"finished": True, "failed": True}})
        # Report failure
        return False

