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

---

Copyright &copy; 2017 Stitch
