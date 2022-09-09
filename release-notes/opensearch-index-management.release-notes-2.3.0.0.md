## Version 2.3.0.0 2022-09-09

Compatible with OpenSearch 2.3.0

### Enhancements
* Replica Count Validation when awareness replica balance is enabled ([#429](https://github.com/opensearch-project/index-management/pull/429))
* Updated detekt plugin and snakeyaml dependency. Updated a code to reduce the number of issues after static analysis ([#483](https://github.com/opensearch-project/index-management/pull/483))
* Transform max_clauses optimization: limit amount of modified buckets being processed at a time and added capping of pageSize to avoid maxClause exception.  ([#477](https://github.com/opensearch-project/index-management/pull/477))
* Remove HOST_DENY_LIST usage as Notification plugin will own it ([#488](https://github.com/opensearch-project/index-management/pull/488))
* Deprecate Master nonmenclature ([#502](https://github.com/opensearch-project/index-management/pull/502))

### Bug Fixes
* Failed concurrent creates of ISM policies should return http 409 ([#464](https://github.com/opensearch-project/index-management/pull/464))
* Disable detekt because to avoid the CVE issues ([#500](https://github.com/opensearch-project/index-management/pull/500))

### Infrastructure
* Staging for version increment automation ([#409](https://github.com/opensearch-project/index-management/pull/409))

### Maintenance
* version upgrade to 2.3.0 ([#484](https://github.com/opensearch-project/index-management/pull/484))

### Documentation
* Added 2.3 release note ([#507](https://github.com/opensearch-project/index-management/pull/507))
