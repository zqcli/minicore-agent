# Verification Evidence

This directory is an evidence index, not the current product or RPC guide.
Records retain the facts, source revisions, gate counts, limitations, and
artifact provenance from the runs they describe. A later package version or
newer acceptance record does not rewrite those historical facts.

## 0.5.0 Local Freeze

- [`0.5.0.md`](0.5.0.md) is the centralized measured freeze record for source
  `8a581a8`; it lists scoped checks, static values, and parent-remote gate
  results. It is the status entry linked from the current guides.

## 0917 Simplification

- [`0917-simplify.md`](0917-simplify.md) records the P0–P7 nonbreaking
  simplification review at source tip `f95a319`. The later `0.5.0` package bump
  is a separate release commit and is not a new behavioral gate in that record.

## 0916 Closeout

- [`0916-closeout.md`](0916-closeout.md) records the preceding shared-data
  closeout and its parent-owned gates; it is superseded as the current source
  status by the 0917 record.
- The closeout names `minicore-agent-0916-implementation-spec.md`, but that
  source specification is not tracked in the current repository. This index does
  not reconstruct it; the closeout record remains the preserved historical
  evidence.

## Earlier Acceptance Records

- [`0914-final.md`](0914-final.md): the 0914 blueprint acceptance map.
- [`0.3.3/README.md`](0.3.3/README.md): the original 0.3.3 release evidence.
- [`session-config.md`](session-config.md): the historical Session/config follow-up.
- [`followups.md`](followups.md): historical Tool/reload/delegation evidence.
- [`reload-refresh.md`](reload-refresh.md): the paired public reload correction.
- [`presentation-risk-fixes.md`](presentation-risk-fixes.md): presentation-path fixes.
- [`compaction.md`](compaction.md) and [`compaction-manual-audit.md`](compaction-manual-audit.md): compaction foundation and incident evidence.

Raw logs, JSON results, manifests, checksums, native captures, and other
artifacts remain in their existing subdirectories and are not moved or rewritten
by this navigation index.
