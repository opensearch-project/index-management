## Version 1.3.0.0 2022-03-08

Compatible with OpenSearch 1.3.0

### Features
* Continuous transforms ([#206](https://github.com/opensearch-project/index-management/pull/206))
* Refactor IndexManagement to support custom actions ([#288](https://github.com/opensearch-project/index-management/pull/288))

### Enhancements
* Adds default action retries ([#212](https://github.com/opensearch-project/index-management/pull/212))
* Adds min rollover age as a transition condition ([#215](https://github.com/opensearch-project/index-management/pull/215))
* Adds min primary shard size rollover condition to the ISM rollover action ([#220](https://github.com/opensearch-project/index-management/pull/220))
* Not managing indices when matched certain pattern ([#255](https://github.com/opensearch-project/index-management/pull/255/files))
* Show applied policy in explain API ([#251](https://github.com/opensearch-project/index-management/pull/251))

### Bug Fixes
* Successful deletes of an index still adds history document ([#160](https://github.com/opensearch-project/index-management/pull/160))
* Porting missing bugfixes ([#232](https://github.com/opensearch-project/index-management/pull/181))
* ISM Template Migration ([#237](https://github.com/opensearch-project/index-management/pull/237))
* Fixes flaky tests ([#211](https://github.com/opensearch-project/index-management/pull/211))
* Fixes flaky rollup/transform explain IT ([#247](https://github.com/opensearch-project/index-management/pull/247))
* Avoids restricted index warning check in blocked index pattern test ([#263](https://github.com/opensearch-project/index-management/pull/263))
* Porting missing logic ([#240](https://github.com/opensearch-project/index-management/pull/240))
* Fixes flaky continuous transforms test ([#276](https://github.com/opensearch-project/index-management/pull/276))
* Porting additional missing logic ([#275](https://github.com/opensearch-project/index-management/pull/275))
* Fixes test failures with security enabled ([#292](https://github.com/opensearch-project/index-management/pull/292))
* Enforces extension action parsers have custom flag ([#306](https://github.com/opensearch-project/index-management/pull/306))

### Infrastructure
* Add support for codeowners to repo ([#195](https://github.com/opensearch-project/index-management/pull/195))
* Adds test and build workflow for mac and windows ([#210](https://github.com/opensearch-project/index-management/pull/210))
* Adding debug log to log the user object for all user callable transport actions ([#166](https://github.com/opensearch-project/index-management/pull/166))
* Added ISM policy backwards compatibility test ([#181](https://github.com/opensearch-project/index-management/pull/181))
* Add backport and auto delete workflow ([#283](https://github.com/opensearch-project/index-management/pull/283))
* Updates integTest gradle scripts to run via remote cluster independently ([#291](https://github.com/opensearch-project/index-management/pull/291))

### Documentation
* Add roadmap badge in README ([#295](https://github.com/opensearch-project/index-management/pull/295))

### Maintenance
* Updating license headers ([#196](https://github.com/opensearch-project/index-management/pull/196))
* Configure WhiteSource for GitHub.com ([#244](https://github.com/opensearch-project/index-management/pull/244))
* Upgrades detekt version to 1.17.1 ([#252](https://github.com/opensearch-project/index-management/pull/252))
* Changes integ test java version from 14 to 11 ([#284](https://github.com/opensearch-project/index-management/pull/284))

