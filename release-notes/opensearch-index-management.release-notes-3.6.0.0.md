## Version 3.6.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.6.0

### Bug Fixes

* Fix flaky rollup test by stopping jobs before index cleanup to prevent race conditions with background coroutines ([#1530](https://github.com/opensearch-project/index-management/pull/1530))
* Fix typo in `validFileNameExcludingAsterisk` validation method ([#1608](https://github.com/opensearch-project/index-management/pull/1608))

### Infrastructure

* Add Remote Store integration test infrastructure with search-only ISM action test (`SearchOnlyActionIT`) ([#1589](https://github.com/opensearch-project/index-management/pull/1589))
* Update shadow plugin usage to replace deprecated Gradle API ([#1587](https://github.com/opensearch-project/index-management/pull/1587))

### Maintenance

* Bump commons-codec:commons-codec from 1.17.2 to 1.21.0 ([#1578](https://github.com/opensearch-project/index-management/pull/1578))
