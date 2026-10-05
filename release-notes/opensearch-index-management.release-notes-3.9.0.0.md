## Version 3.9.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.9.0

### Features

* Add opt-in setting to allow ISM actions to run on a red cluster, enabling recovery-oriented actions like delete while restricting resource-intensive operations ([#1691](https://github.com/opensearch-project/index-management/pull/1691))
* Publish finalized field-domain metadata from ISM for managed write-blocked indices to enhance search shard pruning ([#1719](https://github.com/opensearch-project/index-management/pull/1719))

### Bug Fixes

* Allow mixed raw and rollup search to succeed when a queried field is missing from the rollup index ([#1712](https://github.com/opensearch-project/index-management/pull/1712))

### Infrastructure

* Bump `actions/setup-java` from 5.4.0 to 5.6.0 ([#1700](https://github.com/opensearch-project/index-management/pull/1700))
* Bump `actions/setup-java` from 5.6.0 to 5.7.0 ([#1713](https://github.com/opensearch-project/index-management/pull/1713))
* Bump `actions/setup-java` from 5.7.0 to 6.0.0 ([#1731](https://github.com/opensearch-project/index-management/pull/1731))
* Bump `actions/setup-java` from 6.0.0 to 6.0.1 ([#1735](https://github.com/opensearch-project/index-management/pull/1735))
* Bump `aws-actions/configure-aws-credentials` from 6.2.1 to 6.2.3 ([#1706](https://github.com/opensearch-project/index-management/pull/1706))
