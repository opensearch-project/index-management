## Version 2.8.0.0 2023-06-06

Compatible with OpenSearch 2.8.0

### Maintenance
* Upgrade to gradle 8.1.1. ([#777](https://github.com/opensearch-project/index-management/pull/777))
* Bump version to 2.8. ([#759](https://github.com/opensearch-project/index-management/pull/759))

### Features
* Support notification integration with long running operations. ([#790, 791, 793](https://github.com/opensearch-project/index-management/pull/793))

### Bug fixes
* Remove recursion call when checking permission on indices. ([#785](https://github.com/opensearch-project/index-management/pull/785))
* Added trimming of nanos part of "epoch_millis" timestamp when date_histogram type used is date_nanos. ([#782](https://github.com/opensearch-project/index-management/pull/782))
* Added proper resolving of sourceIndex inside RollupInterceptor, it's required for QueryStringQuery parsing. ([#773](https://github.com/opensearch-project/index-management/pull/773))

### Documentation
* Added 2.8 release notes. ([#794](https://github.com/opensearch-project/index-management/pull/794))