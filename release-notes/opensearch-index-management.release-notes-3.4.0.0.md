## Version 3.4.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.4.0

### Features
* Supporting Exclusion pattern in index pattern in ISM ([#1509](https://github.com/opensearch-project/index-management/pull/1509))

### Bug Fixes
* Fix race condition in rollup start/stop tests ([#1529](https://github.com/opensearch-project/index-management/pull/1529))
* After remove policy from index, coordinator sweep will bind again ([#1525](https://github.com/opensearch-project/index-management/pull/1525))
* Fix snapshot pattern parsing in SM deletion workflow to handle comma-separated values ([#1503](https://github.com/opensearch-project/index-management/pull/1503))

### Infrastructure
* Upgrade gradle to 9.2.0 and github actions JDK 25 ([#1534](https://github.com/opensearch-project/index-management/pull/1534))
* Dependabot: bump actions/github-script from 7 to 8 ([#1485](https://github.com/opensearch-project/index-management/pull/1485))

### Maintenance
* Increments version to 3.4.0 and adds ActionFilter interface ([#1536](https://github.com/opensearch-project/index-management/pull/1536))
* Update logback dependencies to version 1.5.19 ([#1537](https://github.com/opensearch-project/index-management/pull/1537))
* Use optionalField for creation in ExplainSMPolicy serialization ([#1507](https://github.com/opensearch-project/index-management/pull/1507))