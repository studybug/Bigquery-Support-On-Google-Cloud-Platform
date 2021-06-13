# install a pip package
import sys
!{sys.executable} -m pip install pyhmy

!{sys.executable} -m pip3 install pathlib
!{sys.executable} -m pip3 install pyhmy

!pip install requests

!git clone https://github.com/harmony-one/pyhmy.git

!pip install google-cloud-bigquery

import pyhmy
import pandas as pd
import json
import socket
import pytest
import requests

from pyhmy.rpc import (
    exceptions,
    request)

from pyhmy import blockchain

# gets harmony block chain from start block to end block and turns into a pandas dataframe
def get_harmony_blockchain_record():

    # use pyhmy python built api code
    _default_endpoint = 'http://localhost:9500'
    _default_timeout = 30
    blockchain_record = blockchain.get_blocks(0, -1, endpoint=_default_endpoint, include_full_tx=False,
        include_signers=False, timeout=_default_timeout
    )
    blockchain_record = pd.DataFrame(blockchain_record)
    return blockchain_record

blockchain_record = get_harmony_blockchain_record()


# export and update data to google bigguery
def update_dataset_access(dataset_id):

    # [START bigquery_update_dataset_access]
    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set dataset_id to the ID of the dataset to fetch.
    # dataset_id = 'your-project.your_dataset'

    dataset = client.get_dataset(dataset_id)  # Make an API request.

    entry = bigquery.AccessEntry(
        role="READER",
        entity_type="userByEmail",
        entity_id="sample.bigquery.dev@gmail.com",
    )

    entries = list(dataset.access_entries)
    entries.append(entry)
    dataset.access_entries = entries

    dataset = client.update_dataset(dataset, ["access_entries"])  # Make an API request.

    full_dataset_id = "{}.{}".format(dataset.project, dataset.dataset_id)
    print(
        "Updated dataset '{}' with modified user permissions.".format(full_dataset_id)
    )
    # [END bigquery_update_dataset_access]

update_dataset_access(blockchain_record)
