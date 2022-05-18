## Version 2.0.0.0 2022-05-18

Compatible with OpenSearch 2.0.0

### Features
* Adds shrink action to ISM ([#326](https://github.com/opensearch-project/index-management/pull/326))
* Notification integration with IM ([#338](https://github.com/opensearch-project/index-management/pull/338))

### Bug Fixes
* Fix metadata migration logic error when update setting call failed ([#328](https://github.com/opensearch-project/index-management/pull/328))
* Updates search text field to keyword subfield for policies and managed indices ([#267](https://github.com/opensearch-project/index-management/pull/267))
* Fixes shard allocation checks ([#335](https://github.com/opensearch-project/index-management/pull/335))
* BugFix: Notification integration issues ([#339](https://github.com/opensearch-project/index-management/pull/339))
* Fixes flaky continuous transforms and shrink tests ([#340](https://github.com/opensearch-project/index-management/pull/340))
* Minor improvements ([#345](https://github.com/opensearch-project/index-management/pull/345))
* Strengthen scroll search in Coordinator ([#356](https://github.com/opensearch-project/index-management/pull/356))
* Refactors shrink action steps and adds unit tests ([#349](https://github.com/opensearch-project/index-management/pull/349))

### Infrastructure
* Replace checked-in ZIPs with dynamic dependencies ([#327](https://github.com/opensearch-project/index-management/pull/327))
* Only download JS zip when integTest is running ([#334](https://github.com/opensearch-project/index-management/pull/334))

### Documentation
* Updated issue templates from .github. ([#324](https://github.com/opensearch-project/index-management/pull/324))

### Maintenance
* Upgrades Index Management to use 2.0.0-alpha1 of OpenSearch and dependencies ([#318](https://github.com/opensearch-project/index-management/pull/318))
* Make sure qualifier default is alpha1 in IM ([#323](https://github.com/opensearch-project/index-management/pull/323))
* Incremented version to 2.0-rc1. ([#331](https://github.com/opensearch-project/index-management/pull/331))
* Non-inclusive nonmenclature update ([#337](https://github.com/opensearch-project/index-management/pull/337))
* Removes rc1 qualifier ([#353](https://github.com/opensearch-project/index-management/pull/353))
