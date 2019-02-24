# Tranco: A Research-Oriented Top Sites Ranking Hardened Against Manipulation

*By Victor Le Pochat, Tom Van Goethem, Samaneh Tajalizadehkhoob, Maciej Korczyński and Wouter Joosen*

This repository contains the source code driving the generation of the Tranco ranking provided at [https://tranco-list.eu/](). This new top websites ranking was proposed in our paper [Tranco: A Research-Oriented Top Sites Ranking Hardened Against Manipulation](https://tranco-list.eu/assets/tranco-ndss19.pdf).

* `combined_lists.py` contains the core code for generating new lists based on a configuration passed to `combined_lists.generate_combined_list`.
* `shared.py` and `global_config.py` contain several configuration variables; `shared.DEFAULT_TRANCO_CONFIG` gives the configuration of the default (daily updated) Tranco list.
* `generate_daily_list.py` runs daily to generate the default Tranco list.
* `job_handler.py` contains either the code for submitting jobs to an `rq` queue for processing, or code to relay requests for list generation to a remote host.
* `job_server.py` accepts request for list generation on a remote host.
* `notify_email.py` contains code to notify users when their list has been generated.
* `generate_domain_parts.py` preprocesses rankings to extract the different components of domains.