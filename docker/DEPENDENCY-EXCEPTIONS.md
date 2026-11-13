# Vendored dependency security exceptions

Two of this repository's dependencies bundle their own private copies of
`jackson-databind`/`jackson-core` and `lz4-java`, shaded into the jar's own
bytecode under a relocated package. A shaded copy is invisible to Maven's
dependency resolution and to this `pom.xml`'s own `jackson2.version` /
`jackson3.version` / `lz4.version` `dependencyManagement` pins — bumping those
properties has no effect on it. Dependabot/Inspector-style scanners still see
the embedded classes and report the findings below against this repo; they
are open on `dev` today and are not fixable by a version bump here. Recorded
so they aren't re-investigated from scratch the next time a scanner flags
them.

## `gremlin-shaded` (pulled in via `tinkerpop.version`, currently `3.7.7`)

Bundles `jackson-databind`/`jackson-core` **2.18.9**, which carries CVE-2026-83557,
CVE-2026-89425, CVE-2026-91776, CVE-2026-89407, CVE-2026-19032, CVE-2026-91777,
and CVE-2026-68497 (7 findings). Confirmed by downloading and inspecting every
stable TinkerPop release from the current `3.7.7` through the latest `3.8.2`:
all of them bundle the identical `2.18.9` (`3.8.0` actually regresses to
`2.15.2`) — no stable release closes this. The only release that bundles
something different is the pre-release `4.0.0-beta.3`, which relocates Jackson
entirely to `org.apache.tinkerpop.shaded.jackson.*`; adopting a beta dependency
isn't warranted for this alone. Reachability is low: this repo's own TinkerPop
usage doesn't exercise the vulnerable deserialization paths directly, but the
classes are present in the shipped artifact, so scanners will keep flagging
them regardless.

## `databricks-jdbc` (currently `2.8.4`)

Bundles `at.yawk.lz4:lz4-java` **1.11.1**, which carries 5 Inspector2 findings
(CVE-2026-106449/106450/106451/106452/106453 — heap-exhaustion,
predictable-tempfile, and stack-overflow DoS vectors). Unlike the Jackson case above, this one is fully
closed by the `1.x`→`3.x` rewrite: downloading and inspecting
`databricks-jdbc-3.4.4.jar` directly confirms it bundles `lz4-java` **1.11.4**
(fixed), while the current `2.8.4` has no newer `2.x` release to pick up
instead. This repo's own real, directly-declared `lz4-java` dependency
(`lz4.version`) is already pinned to the fixed `1.11.4` with explicit
exclusions forcing every transitive pull through it — only the copy vendored
*inside* the `databricks-jdbc` jar itself is stuck on `1.11.1`. Jumping
`databricks-jdbc` from `2.x` to `3.x` is a separate, untested driver
compatibility change and isn't being proposed here as a side effect of this
note; revisit alongside that migration if/when it's planned.

## Why not just exclude or relocate the shaded classes

Both jars ship Jackson/lz4 unrelocated under their real package names
(`com.fasterxml.jackson.*`, not a shaded prefix), so excluding the bundled
classes via a build-time jar filter would require relocating or re-shading
them ourselves to avoid colliding with the real `jackson2.version`/
`lz4.version` artifacts already on the classpath — more moving parts and a
higher maintenance cost than accepting the exception and revisiting it when
upstream ships a real fix.
