# Changelog

## 0.1.16
  * Adds the "scope" element to the agents schema [#16](https://github.com/singer-io/tap-zendesk-chat/pull/16)

## 0.1.15
  * Update version of `requests` to `2.20.0` in response to CVE 2018-18074

## 0.1.14
  * Add `id` and `departments` fields to the `shortcuts` schema [#13](https://github.com/singer-io/tap-zendesk-chat/pull/13)

## 0.1.13
  * Adds the "scope" element to the shortcuts schema [#12](https://github.com/singer-io/tap-zendesk-chat/pull/12)

## 0.1.11
  * Lowers the chat interval days retrieved to 14 to account for a 10k search result limit [#8](https://github.com/singer-io/tap-zendesk-chat/pull/8)

## 0.1.12
  * Allow chat interval days to be specified as a string in the config [#10](https://github.com/singer-io/tap-zendesk-chat/pull/10)
