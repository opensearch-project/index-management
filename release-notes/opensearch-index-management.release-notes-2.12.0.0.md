## Version 2.12.0.0 2024-02-07

Compatible with OpenSearch 2.12.0

### Maintenance

* Increment version to 2.12.0-SNAPSHOT ([#996](https://github.com/opensearch-project/index-management/pull/996))
* Update to Gradle 8.5 ([#1069](https://github.com/opensearch-project/index-management/pull/1069))
* Support switch aliases in shrink action. ([#987](https://github.com/opensearch-project/index-management/pull/987))

### Enhancements

* Implemented filtering on the ISM eplain API ([#1067](https://github.com/opensearch-project/index-management/pull/1067))
* Add more error notification at fail points ([#1000](https://github.com/opensearch-project/index-management/pull/1000))
* \[Feature] Support Transform as an ISM action ([#760](https://github.com/opensearch-project/index-management/pull/760))
* Set the rollover action to idempotent ([#986](https://github.com/opensearch-project/index-management/pull/986))


### Bug fixes

* GET SM policies return empty list when ism config index does not exist ([#1072](https://github.com/opensearch-project/index-management/pull/1072))
* Added minimum timeout for transforms search of 10 minutes ([#1033](https://github.com/opensearch-project/index-management/pull/1033))
* Interval schedule should take start time from the request, should not… ([#1040](https://github.com/opensearch-project/index-management/pull/1040))
* Added minimum for search.cancel_after_time_interval setting for rollups ([#1026](https://github.com/opensearch-project/index-management/pull/1026))
* Interval schedule should take start time from the request, should not set it to the current time of request execution. ([#1036](https://github.com/opensearch-project/index-management/pull/1036))
* added type check for pipeline aggregator types in Transform initialization ([#1014](https://github.com/opensearch-project/index-management/pull/1014))

### Infrastructure

* Update admin credential in integration test ([#1084](https://github.com/opensearch-project/index-management/pull/1084))
* Onboard jenkins prod docker images to github actions ([#1025](https://github.com/opensearch-project/index-management/pull/1025))
* Improve security plugin enabling check ([#1017](https://github.com/opensearch-project/index-management/pull/1017))

### Documentation

* Version 2.12 Release Notes Draft ([#1092](https://github.com/opensearch-project/index-management/pull/1092))
