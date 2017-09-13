#!/usr/bin/env python
from datetime import datetime, timedelta
import json
import requests
import random
import sys
import uuid

num_to_create = int(sys.argv[1])

visitor_template = """
{{
    "display_name": "gen_visitor {visitor_uuid}",
    "email": "johnnyfake@example.com",
    "notes": "whatever"
}}
"""

chat_template = """
{{
    "visitor": {{
        "id": "{visitor_id}",
        "notes": "i guess this is necessary",
        "email": "",
        "name": "Visitor 18685359"
    }},
    "message":"Hi there this is message {message_num}!",
    "type":"offline_msg",
    "timestamp": {start_timestamp},
    "end_timestamp": {end_timestamp},
    "session": {{
        "browser": "Safari",
        "city": "Orlando",
        "country_code": "US",
        "country_name": "United States",
        "end_date": "{end_str}",
        "ip": "67.32.299.96",
        "platform": "Mac OS",
        "region": "Florida",
        "start_date": "{start_str}",
        "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10) AppleWebKit/600.1.25 (KHTML, like Gecko) Version/8.0 Safari/600.1.25",
        "does_this_accept_anything": "we will see",
        "even_integers?": 234
    }}
}}
"""

with open("config.json") as f:
    token = json.loads(f.read())["access_token"]
    headers = {"Authorization": "Bearer " + token}

payload = visitor_template.format(**dict(
    visitor_uuid=uuid.uuid4().hex,
))
response = requests.post("https://www.zopim.com/api/v2/visitors",
                         headers=headers,
                         data=payload)
response.raise_for_status()
visitor_id = response.json()["id"]
print("created visitor", visitor_id)

for x in range(num_to_create):
    delta = timedelta(seconds=random.randint(0, 60*60*24*5))
    start = datetime.utcnow() - delta
    end   = start + timedelta(minutes=5)
    payload = chat_template.format(**dict(
        visitor_id      = visitor_id,
        email_num       = x % 10,
        message_num     = x,
        start           = start,
        start_timestamp = int(start.timestamp()),
        start_str       = start.isoformat(),
        end             = end,
        end_timestamp   = int(end.timestamp()),
        end_str         = end.isoformat(),
    ))
    response = requests.post("https://www.zopim.com/api/v2/chats",
                             headers=headers,
                             data=payload)
    response.raise_for_status()
    chat_id = response.json()["id"]
    print("created chat", chat_id)
