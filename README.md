# Welcome to SEMOSS

### What is SEMOSS?

SEMOSS stands for Semantic Open Source Software. It is a web application that allows users to build and deploy their custom solutions through a lightweight framework that provides connectors to databases, LLMs, vector stores, storage providers, and custom functions/APIs.

SEMOSS also provides a BI layer for exploring connections in data, creating tailored data products, and running custom algorithms. It can also be used as a data virtualization layer to merge data from multiple databases into a single frame and perform transformations in order to analyze their data.

SEMOSS started as a visualization and analytics tool for RDF data (semantic web) and has evolved over the years into a general purpose platform.


### Developers

For Detailed Commit messages please look [here](hooks/README.md)

### IL4-oriented test container

The `IL4-dev` branch includes a separate, digest-pinned Iron Bank / BC-FIPS
container build for the published SEMOSS 5.4.0 release. See
[the build and deployment guide](docker/il4/README.md) for the CodeBuild runner,
repository secrets, GHCR image, and acceptance requirements.
Development builds refresh the existing Iron Bank version tags; a weekly
Monday schedule becomes active once the workflow is also merged into `dev`.
This build does not establish IL4 authorization or replace the existing
application source builds.