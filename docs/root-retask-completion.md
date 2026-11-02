# Same-root retask completion

Implementation contract for #10 (work in progress).

An optional `completion={assignment_id, artifact_ref}` on an explicit sibling
retask subscribes its durable creator to the settlement of that accepted input.
It is restricted to independent roots. Plain messaging is unchanged. Unsupported
peers reject before message dispatch. Rejection preserves the prior assignment;
acceptance invalidates it. Uncertain delivery is query-only, never a resend.
Settlement is not task acceptance and does not override owner stop.

The supervisor advertises `root_retask_completion_v1`; the sender verifies it
before submitting an opt-in message. The target advertises version 1 in its
existing state response. The supervisor mints `rootRetaskAssignment` on the
existing delivery command; worker peers cannot supply it. Admission rechecks the
exact target, current input digest and goal snapshot. The digest exposes no queued
body. A target task change rejects without replacing its previous subscription.

Reuse the existing root assignment and completion delivery journal. Do not use
child result proof or introduce a scheduler or a second assignment store.
