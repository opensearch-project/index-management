## Version 1.2.0.0 2021-11-05

Compatible with OpenSearch 1.2.0

### Enhancements

* Making snapshot name to scripted input in template ([#77](https://github.com/opensearch-project/index-management/pull/77))
* Adds setting to search all rollup jobs on a target index ([#165](https://github.com/opensearch-project/index-management/pull/165))
* Adds cluster setting to configure index state management jitter ([#153](https://github.com/opensearch-project/index-management/pull/153))
* Allows out of band rollovers on an index without causing ISM to fail ([#180](https://github.com/opensearch-project/index-management/pull/180))

### Bug Fixes

* Fix issues with security changes in rollup runner ([#161](https://github.com/opensearch-project/index-management/pull/161))
* Adds implementation for the delay feature in rollup jobs ([#147](https://github.com/opensearch-project/index-management/pull/147))
* Remove policy API on read only indices ([#182](https://github.com/opensearch-project/index-management/pull/182))
* In explain API not showing the total count to all users ([#185](https://github.com/opensearch-project/index-management/pull/185))

### Infrastructure

* Uses published daily snapshot dependencies ([#141](https://github.com/opensearch-project/index-management/pull/141))
* Removes default integtest.sh ([#148](https://github.com/opensearch-project/index-management/pull/148))
* Adds mavenLocal back to repositories ([#158](https://github.com/opensearch-project/index-management/pull/158))

### Documentation

* Automatically provides license header for new files ([#142](https://github.com/opensearch-project/index-management/pull/142))

### Maintenance

* Updates index management version to 1.2 ([#157](https://github.com/opensearch-project/index-management/pull/157))
* Updates testCompile mockito version, adds AwaitsFix annotation to MetadataRegressionIT tests ([#168](https://github.com/opensearch-project/index-management/pull/168))
