# Private native upgrade boundaries

Keep private behavior in modules owned by the concern. The upstream runner owns
queue pickup, durable input delivery and settlement. It invokes
`worker/turn/owned_delivery.rs` before delivery; that module owns drone admission
and resume fences, owner-control refresh, managed-child start permits and request
progress cadence. A refusal returns to the existing queue parking path. It must
never retry UNKNOWN work or strip the stored permit before durable acceptance.
`worker/update_admission.rs` owns the shared notice/drone drain guard and
update-specific shutdown admission. The upstream snapshot implementation stays
in lifecycle.rs so upstream fixes still merge at their original location.

These are private module boundaries in pa-daemon, with no new dependency, public
API, wire format, storage format, plugin service or alternate runner. The named
admission method deliberately marks an integration boundary even though it has
one production caller; ordinary single-use convenience helpers remain discouraged.
Lock acquisition, await ordering and side effects are preserved from the inline
implementation. The supported upstream seam still needs review when pickup or
settlement semantics change; moving code does not establish semantic compatibility.

The module choice follows the Rust Reference's private descendant access rules:
https://doc.rust-lang.org/reference/visibility-and-privacy.html
Primary-source research used Parallel search `search_03a0c82a32342f16670a8caeedd269e8`.
The concrete comparison was the c6d4da942 upstream turn loop against our integrated
candidate: 550 added lines in turn.rs and 316 in lifecycle.rs before this extraction.
No claim about upgrade duration or token savings follows from the line count.

## Repeatable focused verification

Use a candidate-local Python 3.11+ environment with this checkout's
`prime-agent-runtime` installed editable, dill, the shipped kernel extras and
bundled skill packages. Use the actual TS v0.9.8 CLI as the reference, never a
legacy launcher that targets a removed container. The check preflight verifies
runtime import provenance and TS version before compiling.

```
make private-upgrade-check KERNEL_PYTHON=/absolute/venv/bin/python \
  TS_REFERENCE=/absolute/ts-reference CHECK_OUTPUT=/absolute/new-evidence-directory
```

`CARGO_TARGET_DIR` defaults outside the checkout and contains a literal `target`
component for native test fixture discovery. The source-root stamp refuses cache
reuse from a different checkout and refuses adopting an unowned populated cache.
Use a new target for first adoption; do not share baseline and candidate workspace
crate fingerprints. TZ=UTC keeps the Git golden corpus deterministic. Factory
and orchestration fixtures use the declared kernel, with task-owned output paths.
Each subprocess has a deadline; timeout kills its process group. A filtered suite
with zero executed tests fails. Results record source content digest, actual
commands/exits, imported runtime and reference version in the output directory.

The focused command runs formatting, clippy and existing managed-child, telemetry,
shutdown, update-snapshot, drone, child replacement, roster, Factory and
orchestration checks. It is not full-suite PASS or production qualification.
Existing merge gates and candidate-bound installation/restart checks still apply.
Retain unrelated reproduced baseline failures explicitly rather than silently
removing tests or inventing wider evidence. Add checks here when a new private
consumer changes the dependency closure; keep tests with their owning modules.

## Future merge procedure

Compare upstream changes with these integration calls first. Keep behavior fixes
separate from structural extraction where possible. Preserve the installed
capability inventory and its negative cases, especially UNKNOWN, parent-owned
child reopen, durable input acceptance, cancellation and owner identity. Use the
existing behavioral tests rather than source-shape assertions. Build and package
the complete candidate runtime after checks; never ship only the ELF. Qualification
must use that package and current test binaries, not previous candidate receipts.
