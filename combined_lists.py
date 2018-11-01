import csv
import datetime
import glob
import time
import traceback
from functools import reduce
from itertools import islice
import os

from urllib.parse import urlencode, parse_qs

from hashids import Hashids
from tinydb import TinyDB, Query

import generate_filters

from global_config import *

# Storage of mapping from list ID to list configuration
db = TinyDB('lists.json')

# Hash generation
hsh = Hashids(salt="tsr", min_length=4, alphabet="BCDFGHJKLMNPQRSTVWXYZ23456789")

def count_dict(dct, entry, value=1):
    if not entry in dct:
        dct[entry] = 0
    dct[entry] += value

def date_list(start_date, end_date):
    start_date_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_date_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    return [(start_date_dt + datetime.timedelta(days=x)) for x in range((end_date_dt - start_date_dt).days + 1)]

def _db_id_to_list_id(db_id):
    """ List number to hash """
    return hsh.encode(db_id)

def _list_id_to_db_id(list_id):
    """ Hash to list number """
    try:
        return hsh.decode(list_id)[0]
    except:
        return None

def config_to_list_id(config):
    """ List configuration to list hash (either insert new configuration into database, or retrieve ID for existing list with that configuration) """
    q = Query()
    out = db.search(reduce(lambda x, y: x & y,
                  [getattr(q, k) == v
                   for k, v in config.items()]))
    if out:
        db_id = out[0].doc_id
    else:
        db_id = db.insert({**config, "finished": False, "creationDate": datetime.datetime.now().strftime("%Y-%m-%d"), "creationTime": datetime.datetime.now().isoformat()})
    return _db_id_to_list_id(db_id)

def list_id_to_config(list_id):
    """ Retrieve configuration of existing list based on hash """
    db_id = _list_id_to_db_id(list_id)
    if db_id:
        return {**db.get(doc_id=int(db_id)), "list_id": list_id}

def list_available(list_id):
    """ Check if list is available for download """
    db_id = _list_id_to_db_id(list_id)
    if not db_id:
        return False
    doc = db.get(doc_id=int(db_id))
    return doc is not None and doc.get("finished", False) and not doc.get("failed", True)

def get_generated_list_fp(list_id):
    """ Get file location of existing list """
    return os.path.join(data_path, "generated/{}.csv".format(list_id))

def get_list_fp_for_day(provider, date, parts=False):
    """ Get file location for source list (of one of the providers) """
    date = date.strftime("%Y%m%d")
    if parts:
        fp = next(glob.iglob(os.path.join(data_path, "source/{}/parts/{}_{}*_parts.csv".format(provider, provider, date))))
    else:
        print(os.path.join(data_path, "source/{}/{}_{}*.csv".format(provider, provider, date)))
        fp = next(glob.iglob(os.path.join(data_path, "source/{}/{}_{}*.csv".format(provider, provider, date))))
    return fp

def generate_prefix_items(fp, list_prefix):
    """ Create list of source list items (up to requested list length) """
    with open(fp, encoding='utf8') as f:
        if list_prefix:
            return [r.split(",") for r in islice(f.read().splitlines(), list_prefix)]
        else:
            return [r.split(",") for r in f.read().splitlines()]

# def generate_prefix_items_generator(fp, list_prefix):
#     with open(fp, encoding='utf8') as f:
#         if list_prefix:
#             il = islice(enumerate(f), list_prefix)
#         else:
#             il = enumerate(f)
#         print(il)
#         for l in il:
#             yield (l[0], l[1].rstrip('\n'))

def borda_count_fp(fps, list_prefix):
    """ Generate aggregate scores for domains based on Borda count """
    borda_scores = {}
    for fp in fps:
        items = generate_prefix_items(fp, list_prefix)
        max_score = (list_prefix if list_prefix else 1000000)
        for idx, elem in items:
            count_dict(borda_scores, elem, max_score * (len(items) - int(idx) + 1)/(len(items)))  # necessary to rescale shorter lists (i.e. Quantcast)
    return borda_scores

def dowdall_count_fp(fps, list_prefix):
    """ Generate aggregate scores for domains based on Dowdall count """
    dowdall_scores = {}
    for fp in fps:
        items = generate_prefix_items(fp, list_prefix)
        max_score = (list_prefix if list_prefix else 1000000)
        for idx, elem in items:
            rescaled_rank = max_score * int(idx)/(len(items))
            count_dict(dowdall_scores, elem, 1 / rescaled_rank)  # necessary to rescale shorter lists (i.e. Quantcast)
    return dowdall_scores

def filtered_parts_list(fp, list_prefix, f_pld=None, f_tlds=None, f_organization=None, f_subdomains=None, maintain_rank=True):
    """ Get list of domains that conform to the set filters """
    with open(fp) as f:
        if list_prefix:
            parts_input = islice(f, list_prefix)
        else:
            parts_input = f
        output = []
        organizations_seen = set()
        new_rank = 1
        for line in parts_input:
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
    return output

def get_filtered_parts_lists(fps, input_prefix, config):
    """ Get domains in given source lists that conform to the filters in the configuration """
    for fp in fps:
        yield filtered_parts_list(fp, input_prefix,
                                          config.get("filterPLD", None) == "on",
                                          config.get('filterTLDValue').split(",") if config.get("filterTLDValue",
                                                                                                None) else None,
                                          config.get("filterOrganization", None) == "on",
                                          config.get('filterSubdomainValue').split(",") if config.get(
                                              "filterSubdomainValue", None) else None
                                          )

def borda_count_list(fps, input_prefix, config):
    """ Generate aggregate scores for list of filtered domains based on Borda count """
    borda_scores = {}
    for lst in get_filtered_parts_lists(fps, input_prefix, config):
        max_score = (len(lst) if len(lst) else 1000000)
        for idx, elem in lst:
            count_dict(borda_scores, elem, max_score * (len(lst) - int(idx) + 1)/(len(lst)))  # necessary to rescale shorter lists
    return borda_scores

def dowdall_count_list(fps, input_prefix, config):
    """ Generate aggregate scores for list of filtered domains based on Dowdall count """
    dowdall_scores = {}
    for lst in get_filtered_parts_lists(fps, input_prefix, config):
        max_score = (len(lst) if len(lst) else 1000000)
        for idx, elem in lst:
            rescaled_rank = max_score * int(idx)/(len(lst))
            count_dict(dowdall_scores, elem, 1 / rescaled_rank)  # necessary to rescale shorter lists
    return dowdall_scores

def sort_counts(scores):
    """ Sort domains based on aggregate scores """
    return sorted(scores.keys(), key=lambda elem: scores[elem], reverse=True)

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
        lst = generate_prefix_items(fp, prefix)
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
    return set.union(*map(set, [[i[1] for i in generate_prefix_items(fp, prefix)] for fp in fps]))

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

def write_list_to_file(lst, fp):
    """ Write ranks and domains to file """
    with open(fp, 'w', encoding='utf8') as f:
        csvw = csv.writer(f)
        for idx, entry in enumerate(lst):
            csvw.writerow([idx + 1, entry])


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
        if "filterBlacklists" in config and config['filterBlacklists']:
            domains = generate_filters.filter_blacklist(domains)

        ### OUTPUT ###

        if test:
            return domains
        else:
            # Write list to file
            output_fp = os.path.join(data_path, "generated/{}.csv".format(list_id))
            write_list_to_file(domains, output_fp)

            db.update({"finished": True, "failed": False, "list_id": list_id}, doc_ids=[db_id])
        time.sleep(1)
        # Report success
        return True
    except:
        traceback.print_exc()
        db.update({"finished": True, "failed": True}, doc_ids=[db_id])
        # Report failure
        return False

