# GitHub Actions and Distribution Builds

The workflows in [.github/workflows](../../.github/workflows/) build Java artifacts, language/runtime images, and the assembled SEMOSS application images. The workflow YAML is authoritative for triggers, runner selection, image tags, and publishing conditions.

## Workflow map

| Workflow | Role |
| --- | --- |
| [dev_workflow.yml](../../.github/workflows/dev_workflow.yml) | Java build, test selection, artifact signing, and publication paths |
| [python-builder.yml](../../.github/workflows/python-builder.yml) | Python runtime/dependency image |
| [tomcat-builder.yml](../../.github/workflows/tomcat-builder.yml) | Java/Tomcat builder image |
| [all-docker-builds.yml](../../.github/workflows/all-docker-builds.yml) | Application image build orchestration |
| [ubuntu2204.yml](../../.github/workflows/ubuntu2204.yml), [ubuntu2404.yml](../../.github/workflows/ubuntu2404.yml) | Ubuntu application images |
| [al2023.yml](../../.github/workflows/al2023.yml) | Amazon Linux application image |
| [docker-build-ubi8.yml](../../.github/workflows/docker-build-ubi8.yml), [docker-build-ubi9.yml](../../.github/workflows/docker-build-ubi9.yml) | UBI application images |
| [ubuntu2204_cuda.yml](../../.github/workflows/ubuntu2204_cuda.yml) | CUDA application image |
| [snyk-code.yml](../../.github/workflows/snyk-code.yml) | Source analysis workflow |

## Java artifacts

The current core CI uses a Maven 3.9.9 / Java 21 container. It sets `ci.version` and chooses development or publication paths based on the workflow event and repository configuration. Test execution is affected by the configured skip variable; a successful publishing job is not by itself proof that all integrations ran.

Semoss produces core artifacts, including the `shaded-dependencies` artifact consumed by Monolith. [Monolith's workflow](https://github.com/SEMOSS/Monolith/blob/dev/.github/workflows/monolith_dev_workflow.yml) builds the web application. Keep their artifact versions compatible.

## Runtime and application images

Builder images supply Java/Tomcat and Python components to application image builds. The [Dockerfiles](../../docker/) define the image stages. Distribution assembly also includes frontend and SEMOSS home assets.

Changes to platform agents or skills can span Java code, project descriptors, prompt/skill files, and semoss-ui defaults. Publish and deploy those pieces together. A backend artifact build alone does not establish that the required project assets reached the final image.

## Local validation

Use the [developer onboarding](../development_guides/java_developer_onboarding.md) commands for local Maven verification and [Monolith's local Docker examples](https://github.com/SEMOSS/Monolith#quick-start-with-docker) for integration checks. Image publication requires repository-managed credentials and release configuration; local contributors do not need to run the release profile to validate a change.
