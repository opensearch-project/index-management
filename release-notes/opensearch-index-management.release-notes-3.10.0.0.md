## Version 3.10.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.10.0

### Bug Fixes

* Report SM snapshot creation as FAILED when the snapshot is missing instead of SUCCESS ([#1759](https://github.com/opensearch-project/index-management/pull/1759))
* Resolve plugin-injected index UUID in AttemptTransitionStep so reattached indices are correctly recognized as in-cluster ([#1757](https://github.com/opensearch-project/index-management/pull/1757))
* Resume ISM policies after restoring the config index from a snapshot by reconciling stale index UUIDs ([#1754](https://github.com/opensearch-project/index-management/pull/1754))
* Reconcile policy version mismatches instead of failing the managed index job ([#1746](https://github.com/opensearch-project/index-management/pull/1746))
* Skip initialization of managed index jobs whose document was deleted to prevent orphaned jobs from re-running removed policies ([#1743](https://github.com/opensearch-project/index-management/pull/1743))

### Maintenance

* Update maintainer status for vikasvb90 to emeritus ([#1744](https://github.com/opensearch-project/index-management/pull/1744))
