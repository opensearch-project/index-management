## Version 2.10.0.0 2023-09-07

Compatible with OpenSearch 2.10.0

### Maintenance
* Increment version to 2.10.0-SNAPSHOT. ([#852](https://github.com/opensearch-project/index-management/pull/852))

### Features
* Support copy alias in rollover. ([#892](https://github.com/opensearch-project/index-management/pull/892))
* make control center index as system index. ([#919](https://github.com/opensearch-project/index-management/pull/919))

### Bug fixes
* Fix debug log for missing ISM config index. ([#846](https://github.com/opensearch-project/index-management/pull/846))
* Handle NPE in isRollupIndex. ([#855](https://github.com/opensearch-project/index-management/pull/855))
* fix for max & min aggregations when no metric property exist. ([#870](https://github.com/opensearch-project/index-management/pull/870))
* fix intelliJ IDEA gradle sync error ([#916](https://github.com/opensearch-project/index-management/pull/916))

### Infrastructure
* Add auto github release workflow. ([#691](https://github.com/opensearch-project/index-management/pull/691))
* Fixed the publish maven workflow to execute after pushes to release branches. ([#837](https://github.com/opensearch-project/index-management/pull/837))
* Upgrade the backport workflow. ([#862](https://github.com/opensearch-project/index-management/pull/862))
* Updates demo certs used in integ tests. ([#921](https://github.com/opensearch-project/index-management/pull/921))

### Refactoring
* [Backport 2.x] Fix after core #8157. ([#886](https://github.com/opensearch-project/index-management/pull/886))
* Fix breaking change by core refactor. ([#888](https://github.com/opensearch-project/index-management/pull/888))
* Handle core breaking change. ([#895](https://github.com/opensearch-project/index-management/pull/895))
* Set preference to _primary when searching control-center index. ([#911](https://github.com/opensearch-project/index-management/pull/911))
* Add primary first preference to all search requests. ([#912](https://github.com/opensearch-project/index-management/pull/912))

### Documentation
* Added 2.10 release notes. ([#925](https://github.com/opensearch-project/index-management/pull/925))