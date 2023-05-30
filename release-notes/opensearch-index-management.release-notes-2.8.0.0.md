## Version 2.8.0.0 2023-06-06

Compatible with OpenSearch 2.8.0

### Maintenance
* Upgrade to gradle 8.1.1. ([#777](https://github.com/opensearch-project/index-management/pull/777))
* Bump version to 2.8. ([#759](https://github.com/opensearch-project/index-management/pull/759))

### Features
* Support notification integration with long running operations. ([#712](https://github.com/opensearch-project/index-management/pull/712), [#722](https://github.com/opensearch-project/index-management/pull/722))

### Bug fixes
* Remove recursion call when checking permission on indices. ([#779](https://github.com/opensearch-project/index-management/pull/779))
* Added trimming of nanos part of "epoch_millis" timestamp when date_histogram type used is date_nanos. ([#772](https://github.com/opensearch-project/index-management/pull/772))
* Added proper resolving of sourceIndex inside RollupInterceptor, it's required for QueryStringQuery parsing. ([#764](https://github.com/opensearch-project/index-management/pull/764))

### Documentation
* Added 2.8 release notes. ([#794](https://github.com/opensearch-project/index-management/pull/794))