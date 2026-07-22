## Version 3.8.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.8.0

### Features

* Support mixed AND/OR rollover conditions via `any_of` grouped syntax ([#1667](https://github.com/opensearch-project/index-management/pull/1667))

### Enhancements

* Onboard code diff analyzer/reviewer and issue dedupe workflows ([#1685](https://github.com/opensearch-project/index-management/pull/1685))
* Onboard new backport-pr reusable GitHub workflow ([#1678](https://github.com/opensearch-project/index-management/pull/1678))

### Bug Fixes

* Fix NoClassDefFoundError in ISM custom webhook error notifications by adding HttpClient 5 dependencies ([#1643](https://github.com/opensearch-project/index-management/pull/1643))
* Fix flaky NotificationActionListenerIT zero-notification assertions by waiting past the 5-second webhook delay ([#1688](https://github.com/opensearch-project/index-management/pull/1688))
* Fix flaky SMRunnerIT deletion pattern test by asserting only on pattern-matched snapshots ([#1692](https://github.com/opensearch-project/index-management/pull/1692))
* Fix Rollup/Transform security ITs failing after own_index removal by granting cluster-level bulk permission ([#1666](https://github.com/opensearch-project/index-management/pull/1666))

### Infrastructure

* Add CI mirror repository to avoid Maven Central throttling during builds ([#1650](https://github.com/opensearch-project/index-management/pull/1650))
* Update opensearch-build workflow references from commit SHA to main branch ([#1672](https://github.com/opensearch-project/index-management/pull/1672))
* Replace deprecated tibdex/github-app-token with actions/create-github-app-token ([#1663](https://github.com/opensearch-project/index-management/pull/1663))
* Remove unused release-drafter workflow ([#1686](https://github.com/opensearch-project/index-management/pull/1686))
* Bump actions/checkout from 6.0.3 to 7.0.0 ([#1674](https://github.com/opensearch-project/index-management/pull/1674))
* Bump actions/download-artifact from 7.0.0 to 8.0.1 ([#1653](https://github.com/opensearch-project/index-management/pull/1653))
* Bump actions/setup-java from 5.2.0 to 5.4.0 ([#1684](https://github.com/opensearch-project/index-management/pull/1684))
* Bump aws-actions/configure-aws-credentials from 6.1.1 to 6.2.1 ([#1683](https://github.com/opensearch-project/index-management/pull/1683))
* Bump codecov/codecov-action from 4.6.0 to 7.0.0 ([#1682](https://github.com/opensearch-project/index-management/pull/1682))
* Bump release-drafter/release-drafter from 6.4.0 to 7.3.1 ([#1658](https://github.com/opensearch-project/index-management/pull/1658))

### Maintenance

* Add @Tarun-kishore as a maintainer ([#1689](https://github.com/opensearch-project/index-management/pull/1689))
