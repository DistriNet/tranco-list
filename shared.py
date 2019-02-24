DATE_FORMAT_WITH_HYPHEN = "%Y-%m-%d"
DEFAULT_TRANCO_CONFIG = {"nbDays": "30", "nbDaysFrom": "end",
                  "combinationMethod": "dowdall",  # TODO make choice based on assessment on stability etc.
                  "listPrefix": 'full',
                  "includeDomains": 'all',  # TODO make choice
                  "filterPLD": "on",
                  "providers": ["alexa", "umbrella", "majestic", "quantcast"]
        }
ZIP_FILENAME_FORMAT = "tranco_{}-1m.csv.zip"