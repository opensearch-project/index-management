# Field-domain computation permissions

The `publish_field_domains` action computes summaries of the entire managed index, including documents and fields hidden from the policy user by document-level security, field-level security, or field masking.

The policy execution identity needs two separate index permissions:

- `indices:admin/opensearch/ism/field_domains/compute` authorizes obtaining complete summaries of supported fields on the specified index. It is checked before refresh or privileged searches, including for empty fields.
- `indices:admin/field_domains/put` authorizes publishing those summaries through the existing core API. Publication is authorized independently under the policy identity.

Grant computation only to trusted publishing identities on the intended concrete indices or index patterns. It deliberately authorizes index-wide summaries beyond that identity's ordinary search visibility. It does not grant access to document contents or permit caller-supplied queries, scripts, or search sources. Ordinary search access and permission to manage an ISM policy do not implicitly authorize computation.

Wildcard action grants that cover the computation action name also authorize it. Audit those grants before enabling publication for restricted policy users.

The computation request contains one concrete index identity (name and UUID) and supported field configurations. The server validates the index, requires its write block, refreshes under the policy identity, and runs internally constructed searches under the ISM plugin identity. Failed, partial, timed-out, or early-terminated searches cannot produce published metadata. Only encoded domains are returned, never the privileged search responses.

The plugin's additional search permission covers all indices because its permission file is static. The computation action is the authorization boundary; the shared search context confines calculator searches to that action's concrete index. Plugin privileges are scoped to search dispatch and restored afterward.

The policy user still needs refresh and lifecycle permissions. An identity allowed to compute but not publish can obtain summaries through the computation action but cannot write them to index metadata. Empty, authorized computations do not publish anything.

This change does not protect existing metadata-reading APIs or search-side inference channels. Those are separate core/security changes. Finalized domains also retain their lifecycle requirement: publish only for indices that will remain write-blocked.
