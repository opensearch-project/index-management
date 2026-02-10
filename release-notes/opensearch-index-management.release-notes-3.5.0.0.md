## Version 3.5.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.5.0

### Features

* Add optional `rename_pattern` parameter to convert_index_to_remote action ([#1568](https://github.com/opensearch-project/index-management/pull/1568))
* Add search_only ISM action for Reader/Writer Separation ([#1560](https://github.com/opensearch-project/index-management/pull/1560))
* Adding Cardinality as supported metric for Rollups ([#1567](https://github.com/opensearch-project/index-management/pull/1567))
* Adding support for multi-tier rollups in ISM ([#1533](https://github.com/opensearch-project/index-management/pull/1533))

### Bug Fixes

* Fix uncaught exception in explain API when invalid sortOrder is provided ([#1563](https://github.com/opensearch-project/index-management/pull/1563))
* Fix: don't call Alias API if aliasActions are empty ([#1489](https://github.com/opensearch-project/index-management/pull/1489))

### Infrastructure

* Improve CI speed by refactoring RollupActionIT ([#1572](https://github.com/opensearch-project/index-management/pull/1572))
* Dependabot: bump actions/download-artifact from 5 to 7 ([#1549](https://github.com/opensearch-project/index-management/pull/1549))
* Dependabot: bump actions/upload-artifact from 4 to 6 ([#1551](https://github.com/opensearch-project/index-management/pull/1551))

### Maintenance

* Change min version for supporting source index in ISM rollups to 3.5.0 ([#1573](https://github.com/opensearch-project/index-management/pull/1573))
* Dependabot: bump ch.qos.logback:logback-classic from 1.5.22 to 1.5.23 ([#1554](https://github.com/opensearch-project/index-management/pull/1554))
* Dependabot: bump ch.qos.logback:logback-core from 1.5.23 to 1.5.24 ([#1569](https://github.com/opensearch-project/index-management/pull/1569))
* Dependabot: bump commons-beanutils:commons-beanutils from 1.10.1 to 1.11.0 ([#1562](https://github.com/opensearch-project/index-management/pull/1562))
* Dependabot: bump org.jacoco:org.jacoco.agent from 0.8.12 to 0.8.14 ([#1505](https://github.com/opensearch-project/index-management/pull/1505))