# Same-root retask completion

Implementation contract for #10 (work in progress).

An optional `completion={assignment_id, artifact_ref}` on an explicit sibling
retask subscribes its durable creator to the settlement of that accepted input.
It is restricted to independent roots. Plain messaging is unchanged. Unsupported
peers reject before message dispatch. Rejection preserves the prior assignment;
acceptance invalidates it. Uncertain delivery is query-only, never a resend.
Settlement is not task acceptance and does not override owner stop.

Reuse the existing root assignment and completion delivery journal. Do not use
child result proof or introduce a scheduler or a second assignment store.
