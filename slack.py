import dlt
from dlt.sources.helpers import requests
from slack import slack_source




def load_all_data() -> None:
    """
    This demo script uses the resources with incremental
    loading based on "merge" mode to load all data from channels conversations, replies,
    and incrementally.
    """
    pipeline = dlt.pipeline(
        pipeline_name='slack23', destination='bigquery', dataset_name='slack_data_dlt_all'
    )
    data = slack_source()
    
    load_info = pipeline.run(data)

    print(load_info)


if __name__ == "__main__":
    load_all_data()
