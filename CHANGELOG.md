# Changelog

## 0.5.1
  * Bug Fix with chats stream [#54](https://github.com/singer-io/tap-zendesk-chat/pull/54)
  * Dependabot update

## 0.5.0
  * Zendesk Domain Change [#52](https://github.com/singer-io/tap-zendesk-chat/pull/52)

## 0.4.1
  * Dependabot update [#50](https://github.com/singer-io/tap-zendesk-chat/pull/50)

## 0.4.0
  * Code Refactoring [#45](https://github.com/singer-io/tap-zendesk-chat/pull/45)
    - Improved directory structure
    - Added pagination support to BANS stream
    - Adding Missing fields in `chats`, `shortcuts` & `bans` stream
    - Upodated unit tests
    - pre-commit integrated

  * Fixes Following issues
    - https://github.com/singer-io/tap-zendesk-chat/issues/43
    - https://github.com/singer-io/tap-zendesk-chat/issues/22
    - https://github.com/singer-io/tap-zendesk-chat/issues/21
    - https://github.com/singer-io/tap-zendesk-chat/issues/17

  * Added Integration Tests [#47](https://github.com/singer-io/tap-zendesk-chat/pull/47)


## 0.3.2
  * Resolved transform error [#41](https://github.com/singer-io/tap-zendesk-chat/pull/41)
## 0.3.1
  * Adds `items` key to the array schemas [#34](https://github.com/singer-io/tap-zendesk-chat/pull/34)
## 0.3.0
  * Adds field selection via transformer and increases singer-python version to `5.12.1` [#32](https://github.com/singer-io/tap-zendesk-chat/pull/32)
  * Adds bookmarking test [#31](https://github.com/singer-io/tap-zendesk-chat/pull/31)
  * Fixes schemas to not include `additionalProperties` and adds start date test[#30](https://github.com/singer-io/tap-zendesk-chat/pull/30)
  * Changes metadata to allow `inclusion: available` to support field selection and adds discovery test[#29](https://github.com/singer-io/tap-zendesk-chat/pull/29)

## 0.2.1
  * Added `enabled_departments` to the JSON Schema for agents [#27](https://github.com/singer-io/tap-zendesk-chat/pull/27)

## 0.2.0
  * Migrated annotated-schema to use metadata instead [#20](https://github.com/singer-io/tap-zendesk-chat/pull/20)

## 0.1.17
  * Fixes a 400 Bad Request issue when fetching Agents [#19](https://github.com/singer-io/tap-zendesk-chat/pull/19)

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