import dlt
import slack_sdk
from dlt.sources.helpers import requests
from dlt.extract.source import DltResource
from dlt.common.typing import TDataItem, TDataItems
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from typing import Any, Dict, Generator, Optional, Tuple, Iterable
import time
import datetime

@dlt.source(name="slack")
def slack_source(
    api_secret_key=dlt.secrets.value, 
    start_time : Optional[Any] = dlt.config.value, 
    end_time : Optional[Any] =  dlt.config.value,
):
    
    if start_time:
        start_time = time.mktime(datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S").timetuple())
        
    # if end_time is "NOW", get the current GMT time and convert it to UNIX timestamp
    if end_time == "NOW":
        end_time = time.mktime(datetime.datetime.utcnow().timetuple())
    else:
        # else, convert the provided end_time to UNIX timestamp
        end_time = time.mktime(datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S").timetuple())
    
    # call the slack_resource function
    yield slack_resource(api_secret_key, start_time, end_time)


@dlt.resource(write_disposition="merge")
def slack_resource(api_secret_key=None, 
                   start_time=None,
                   end_time=None)  -> Iterable[TDataItem]:

    state = dlt.current.source_state()
    # If "since_time" is not in state or start_time is greater than state["since_time"], update state["since_time"]
    if "since_time" not in state or start_time > state["since_time"]:
        state["since_time"] = start_time
    # Use the greater of start_time and state["since_time"] as start_time
    start_time = max(start_time, state["since_time"])
    state["since_time"] = end_time  # Update state["since_time"] with end_time after each run

    print(state["since_time"])
    client = WebClient(token=api_secret_key)
    print(start_time, end_time)
    try:
        response = client.conversations_list()
        channels = response['channels']
        print(f'You are a Member of {len(channels)} channels:')
        for channel in channels:
            print(channel['name'])
    except SlackApiError as e:
        print("Error : {}".format(e))
    
    channel_ids = dlt.config['slack']['channel_ids']
    

    for channel_id in channel_ids:
        cursor = None  # Initialize cursor for pagination
        while True:  # Keep calling conversations_history until has_more is False
            try:
                channel_info = client.conversations_info(channel=channel_id)
                channel_name = channel_info['channel']['name']

                result = client.conversations_history(channel=channel_id, oldest=start_time, latest=end_time, cursor=cursor)
                conversation_history = result["messages"]
                print("{} messages found in channel '{}': {}".format(len(conversation_history), channel_name, channel_id))
                for conversation in conversation_history:
                    user_id = conversation['user']
                    user_email = None
                    try:
                        user_info = client.users_info(user=user_id)
                        user_email = user_info['user']['profile'].get('email', None)
                    except SlackApiError as e:
                        print(f"Error fetching user info: {e}")

                    message_reactions = []
                    if 'reactions' in conversation:
                        for reaction in conversation['reactions']:
                            message_reactions.append({
                                'name': reaction['name'],
                                'count': reaction['count'],
                                'users': reaction['users']
                            })

                    # Fetching replies for the message
                    replies = []
                    if 'thread_ts' in conversation:
                        try:
                            result = client.conversations_replies(channel=channel_id, ts=conversation['thread_ts'])
                            for reply in result['messages']:
                                user_email_reply = None
                                try:
                                    user_info_reply = client.users_info(user=reply['user'])
                                    user_email_reply = user_info_reply['user']['profile'].get('email', None)
                                except SlackApiError as e:
                                    print(f"Error fetching user info for reply: {e}")
                                replies.append({
                                    'user': reply['user'],
                                    'message': reply['text'],
                                    'user_email': user_email_reply
                                })
                        except SlackApiError as e:
                            print("Error fetching conversation replies: {}".format(e))

                    conversation['user_email'] = user_email
                    conversation['message_reactions'] = message_reactions
                    conversation['replies'] = replies
                    conversation['channel_name'] = channel_name

                    yield conversation

                if not result['has_more']:  # If has_more is False, exit the loop
                    break
                cursor = result['response_metadata']['next_cursor']  # Get next cursor for pagination

            except SlackApiError as e:
                print("Error creating conversation: {}".format(e))
                break  # If there's an error, exit the loop