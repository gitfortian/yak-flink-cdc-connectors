# Future Yak Ops integration boundary

Yak Ops may later call the Phase-0 runtime gateway described in
`yak-cdc-runtime/README.md`. It should treat the gateway as an external deployment service and keep
the existing offline synchronization path unchanged. This phase adds no Yak Ops business model,
scheduler, UI, migration, or live-sync orchestration.

Yak Ops must pass secret **references**, never resolved credentials, and must not copy a resolved
Pipeline definition into its task database or audit log. It should discover exact capabilities,
require `deliverySemantics=at-least-once`, validate before deploy, retain the returned opaque job ID,
and use status/stop/logs only for that deployment. Future authentication and tenant authorization
must be designed before exposing the gateway outside a trusted control network.
