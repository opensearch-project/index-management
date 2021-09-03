## Version 1.1.0.0 2021-09-03

Compatible with OpenSearch 1.1.0

### Infrastructure

* Upgrade dependencies to 1.1 and build snapshot by default. ([#121](https://github.com/opensearch-project/index-management/pull/121))

### Features

* Storing user information as part of the job when security plugin is installed ([#113](https://github.com/opensearch-project/index-management/pull/113))
* Storing user object in all APIs and enabling filter of response based on user ([#115](https://github.com/opensearch-project/index-management/pull/115))
* Security improvements ([#126](https://github.com/opensearch-project/index-management/pull/126))
* Updating security filtering logic ([#137](https://github.com/opensearch-project/index-management/pull/137))

### Enhancements

* Enhance ISM template ([#105](https://github.com/opensearch-project/index-management/pull/105))

### Bug Fixes

* Removing Usages of Action Get Call and using listeners ([#100](https://github.com/opensearch-project/index-management/pull/100))
* Explain response still use old opendistro policy id ([#109](https://github.com/opensearch-project/index-management/pull/109))
