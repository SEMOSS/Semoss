# Welcome to SEMOSS

### What is SEMOSS?

SEMOSS stands for Semantic Open Source Software. It is a web application that allows users to build and deploy their custom solutions through a lightweight framework that provides connectors to databases, LLMs, vector stores, storage providers, and custom functions/APIs.

SEMOSS also provides a BI layer for exploring connections in data, creating tailored data products, and running custom algorithms. It can also be used as a data virtualization layer to merge data from multiple databases into a single frame and perform transformations in order to analyze their data.

SEMOSS started as a visualization and analytics tool for RDF data (semantic web) and has evolved over the years into a general purpose platform.


### Developers

For Detailed Commit messages please look [here](hooks/README.md)

### Experimental ACCP comparison container

The `IL4-dev-ACCP` branch builds an experimental ACCP-FIPS / SunJSSE container
for the published SEMOSS 5.4.0 release, independently of the BC-FIPS image on
`IL4-dev`. See
[the build and deployment guide](docker/il4/README.md) for the CodeBuild runner,
repository secrets, GHCR image, and acceptance requirements.
Development builds refresh the existing Iron Bank version tags and publish to
`ghcr.io/semoss/semoss-il4-accp`. This branch has no weekly schedule.
The guide also includes an opt-in hardened development Compose profile,
CA-verified readiness checks, bounded local logs, and optional report-only
SBOM/vulnerability artifacts. A concurrent, isolated BC/ACCP comparison must
pass before publication, including native administrator login and real SEMOSS
registration/query checks against disposable TLS PostgreSQL and MariaDB servers.
Existing `IL4-dev` deployments are unchanged.
This candidate is **NONVALIDATED**: PBKDF2/PKCS12 use SunJCE fallback and exact
module certificate coverage remains unresolved. Startup requires explicit
development acknowledgment. It does not establish FIPS compliance or IL4
authorization, and it does not replace the existing application source builds.