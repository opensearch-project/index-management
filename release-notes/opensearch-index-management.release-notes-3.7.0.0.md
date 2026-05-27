## Version 3.7.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.7.0

### Features

* Add `reload_cached_resources` query parameter to `_refresh_search_analyzers` API for hot-reloading cached token filter resources (e.g., hunspell dictionaries) without node restart ([#1638](https://github.com/opensearch-project/index-management/pull/1638))

### Bug Fixes

* Fix `_refresh_search_analyzers` failing with 403 on indices with METADATA_WRITE block such as CCR follower indices ([#1635](https://github.com/opensearch-project/index-management/pull/1635))
* Don't fail transition condition verification when an empty conditions set is provided ([#1626](https://github.com/opensearch-project/index-management/pull/1626))

### Infrastructure

* Add issues write permission to untriaged label workflow to fix 403 error when applying labels ([#1645](https://github.com/opensearch-project/index-management/pull/1645))
* Fix flaky `RestGetRollupActionIT` and `RestGetTransformActionIT` by resetting cluster settings and retrying through transient shard recovery ([#1644](https://github.com/opensearch-project/index-management/pull/1644))
* Pin GitHub Actions to commit SHAs to prevent supply chain attacks ([#1648](https://github.com/opensearch-project/index-management/pull/1648))
* Pin `actions/github-script` to exact commit SHA for immutable CI references ([#1647](https://github.com/opensearch-project/index-management/pull/1647))
* Fix flaky `test multi-tier rollup with cardinality` by using `Locale.ROOT` for timestamp formatting and adding bulk response error checking ([#1574](https://github.com/opensearch-project/index-management/pull/1574))
* Fix flaky multi-node ISM tests by waiting for cluster GREEN health before index cleanup to prevent node lock corruption ([#1575](https://github.com/opensearch-project/index-management/pull/1575))

### Documentation

* Update affiliation to Apple for Shivansh Arora ([#1627](https://github.com/opensearch-project/index-management/pull/1627))

### Maintenance

* Bump index management to OpenSearch 3.7 and update Gradle wrapper to 9.4.1 ([#1640](https://github.com/opensearch-project/index-management/pull/1640))
* Bump `1password/load-secrets-action` from 3 to 4 ([#1611](https://github.com/opensearch-project/index-management/pull/1611))
* Bump `aws-actions/configure-aws-credentials` from 5 to 6 ([#1583](https://github.com/opensearch-project/index-management/pull/1583))
* Bump `ch.qos.logback:logback-core` from 1.5.26 to 1.5.32 ([#1599](https://github.com/opensearch-project/index-management/pull/1599))
