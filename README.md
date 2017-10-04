# tap-zendesk-chat

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from Zendesk's Chat [API](https://developer.zendesk.com/rest_api/docs/chat/introduction)
- Extracts the following resources:
  - [Account](https://developer.zendesk.com/rest_api/docs/chat/accounts)
  - [Agents](https://developer.zendesk.com/rest_api/docs/chat/agents)
  - [Bans](https://developer.zendesk.com/rest_api/docs/chat/bans)
  - [Chats](https://developer.zendesk.com/rest_api/docs/chat/chats)
  - [Departments](https://developer.zendesk.com/rest_api/docs/chat/departments)
  - [Goals](https://developer.zendesk.com/rest_api/docs/chat/goals)
  - [Shortcuts](https://developer.zendesk.com/rest_api/docs/chat/shortcuts)
  - [Triggers](https://developer.zendesk.com/rest_api/docs/chat/triggers)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Quick Start

1. Install

    pip install tap-zendesk-chat

2. Get a Zendesk Chat Access Token

You will need to be able to create a token using an OAuth 2 authorization
request. To do this locally, you can use
[Postman](https://chrome.google.com/webstore/detail/postman/fhbjgbiflinjbdggehcddcbncdddomop).

Once you know how you will authorize (including the redirect URLs for the
authorization request), log into your Zendesk Chat / Zopim account, go to

    Settings -> Account -> API -> Add API Client

Once you create the API Client you will receive a client ID and client secret.
Use these in conjunction with your chose method of performing the OAuth 2
reqeust to obtain an access token to your (or a third-party) Zendesk Chat /
Zopim account.

3. Create the Config File

Create a JSON file called `config.json` containing the access token and a
`start_date`, which specifies the date at which the tap will begin pulling data
(for those resources that support this).

```json
{
    "start_date": "2010-01-01",
    "access_token": "your-access-token"
}
```

4. Run the Tap in Discovery Mode

    tap-zendesk-chat -c config.json -d

See the Singer docs on discovery mode
[here](https://github.com/singer-io/getting-started/blob/master/BEST_PRACTICES.md#discover-mode-and-connection-checks).

5. Run the Tap in Sync Mode

    tap-zendesk-chat -c config.json -p catalog-file.json

## Chats Full Re-syncs

You can configure the tap to re-sync all chats every so many number of days.
This is configured with the `chats_full_sync_days` option in your `config.json`
file.

This exists due to the options (or lack thereof) the Zendesk Chat API provides
when syncing chats. Each chat has an "end timestamp" that indicates when the
chat was "ended." However, even after the chat has ended, it may be modified.
In order to not sync all of the chats during every run of the tap, the tap
filters data based on the "end timestamp." But this means if the chat is
modified after the tap has already synced it, any modifications will be missed
by the tap. By re-syncing every N days, you are able to update any chats that
may have changed since they were synced by the tap.

## Two Bookmarks for Chats

In addition to the above oddities around the "end timestamp," the chats
resource provides another unique challenge. There are two different types of
chats: offline messages and normal chats.

Offline messages do not have the end timestamp property. Instead, they only
provide a timestamp when the offline message was created. As a result, the tap
syncs offline timestamps based on their timestamp and syncs regular chats based
on their end timestamp. This requires storing two separate bookmarks in the
tap's "state."

---

Copyright &copy; 2017 Stitch
