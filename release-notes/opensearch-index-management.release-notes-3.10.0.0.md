## Version 3.10.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.10.0

### Bug Fixes

* Resume ISM policies after restoring the config index from a snapshot ([#1754](https://github.com/opensearch-project/index-management/pull/1754))
* Reconcile policy version mismatches instead of failing the managed index job ([#1746](https://github.com/opensearch-project/index-management/pull/1746))
* Skip initialization of orphaned managed index jobs whose document was deleted ([#1743](https://github.com/opensearch-project/index-management/pull/1743))
* Resolve plugin-injected index UUID in AttemptTransitionStep for re-attached indices ([#1757](https://github.com/opensearch-project/index-management/pull/1757))
* Report Snapshot Management snapshot creation as failed when the snapshot is missing ([#1759](https://github.com/opensearch-project/index-management/pull/1759))

### Maintenance

* Update maintainer status for vikasvb90 to emeritus ([#1744](https://github.com/opensearch-project/index-management/pull/1744))
