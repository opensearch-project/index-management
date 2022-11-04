## Version 2.4.0.0 2022-11-04

Compatible with OpenSearch 2.4.0

### Enhancements
* Feature/184 introduce security tests ([#474](https://github.com/opensearch-project/index-management/pull/474))
* alias in rollup target_index field ([#445](https://github.com/opensearch-project/index-management/pull/445))
* Adds an alias action ([#575](https://github.com/opensearch-project/index-management/pull/575))
* Error prevention / Action validation stage 1 ([#579](https://github.com/opensearch-project/index-management/pull/579))

### Bug Fixes
* Added rounding when using aggregate script for avg metric. ([#490](https://github.com/opensearch-project/index-management/pull/490))
* Adds plugin version sweep background job ([#434](https://github.com/opensearch-project/index-management/pull/434))
* Moved _doc_count from transform._doc_count to root of document ([#558](https://github.com/opensearch-project/index-management/pull/558))
* Bugfix/538 Adding timeout and retry to Transform '_search' API calls ([#576](https://github.com/opensearch-project/index-management/pull/576))

### Infrastructure
* add group = org.opensearch.plugin ([#571](https://github.com/opensearch-project/index-management/pull/571))

### Maintenance
* Fix kotlin warnings ([#551](https://github.com/opensearch-project/index-management/pull/551))
* Update jackson to 2.13.4 ([#557](https://github.com/opensearch-project/index-management/pull/557))
* Increment version to 2.4.0-SNAPSHOT ([#573](https://github.com/opensearch-project/index-management/pull/573))
* Fix the compatibility issue of awareness replica validation ([#595](https://github.com/opensearch-project/index-management/pull/595))

### Documentation
* 2.4 release note ([#598](https://github.com/opensearch-project/index-management/pull/598))
