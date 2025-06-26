# GitHub Actions Workflows for SEMOSS CI/CD

SEMOSS utilizes GitHub Actions to automate its Continuous Integration and Continuous Deployment (CI/CD) processes. These workflows handle tasks such as building the Java backend, creating Docker images for various components and the final application, and managing releases. This document outlines the key workflows found in the `.github/workflows/` directory.

*(Note: `auto-comment.yml` and `pr-close-comment.yml` are utility workflows and are not covered here.)*

## 1. Main CI Workflow (`dev_workflow.yml`)

*   **Name**: `CI`
*   **Purpose**: This is the primary workflow for continuous integration of the SEMOSS Java backend. It handles building, optionally testing, and deploying SNAPSHOT or RELEASE versions of the application.
*   **Triggers**:
    *   Push to `dev` or `main` branches.
    *   Pull request targeting `dev` or `main` branches.
    *   Manual trigger (`workflow_dispatch`).
*   **Key Jobs**:
    *   **`build_dev`**:
        *   **Context**: Runs for pushes and pull requests to `dev`/`main` when not a release (`vars.RELEASE != 'TRUE'`).
        *   **Environment**: Executes on a self-hosted runner within a `semoss/docker` container.
        *   **Steps**:
            1.  Checks out the source code.
            2.  Installs `openjfx` (JavaFX dependency).
            3.  Builds the Java project using Maven: `mvn clean install`.
                *   Test execution can be skipped based on the `vars.SKIP_TESTS` variable.
                *   Builds a SNAPSHOT version (e.g., `${vars.VERSION}-SNAPSHOT`).
            4.  Cleans up the local Maven repository for `org/semoss` to save space.
    *   **`build_deploy`**:
        *   **Context**: Runs on manual `workflow_dispatch` when not a release (`vars.RELEASE != 'TRUE'`).
        *   **Environment**: Self-hosted runner, `semoss/docker` container.
        *   **Steps**:
            1.  Checks out code.
            2.  Installs `openjfx` and `gnupg2`.
            3.  Imports a GPG key (from `secrets.GPG_PRIVATE_KEY_2`) for signing.
            4.  Deploys with Maven:
                *   If on `dev` or `main` branch: `mvn deploy -P deploy ...` (activates "deploy" profile, builds SNAPSHOT).
                *   If on other branches (e.g., feature branches): `mvn clean install ...` (builds SNAPSHOT locally).
            5.  If not `dev`/`main`, uploads the JAR to a Nexus repository (`artifact.semoss.org`) using `curl` and credentials from `secrets.NEXUS_USERNAME`/`secrets.NEUXS_PASSWORD`.
    *   **`build_release`**:
        *   **Context**: Runs on manual `workflow_dispatch` when it is a release (`vars.RELEASE == 'TRUE'`).
        *   **Environment**: Self-hosted runner, `semoss/docker` container.
        *   **Steps**:
            1.  Similar setup to `build_deploy` (checkout, dependencies, GPG key).
            2.  Deploys with Maven: `mvn deploy -P deploy ... -Dci.version=${vars.VERSION}` (builds a final release version, not a SNAPSHOT).
*   **Notes**:
    *   The workflow relies heavily on GitHub Actions `vars` (like `RELEASE`, `VERSION`, `SKIP_TESTS`) and `secrets` (for GPG keys, Nexus credentials).
    *   The use of a `semoss/docker` container implies a pre-configured build environment with necessary tools like Java and Maven.
    *   Commented-out sections suggest previous integration with SVN for version file management.

## 2. Python Environment Builder (`python-builder.yml`)

*   **Name**: `Python Builder`
*   **Purpose**: Builds the SEMOSS Python environment Docker image (`quay.io/semoss/python-builder`) which is then used as a layer in the main SEMOSS application images.
*   **Triggers**:
    *   Manual trigger (`workflow_dispatch`) with inputs for Python version, compute type (cpu/gpu), and base Ubuntu image details.
    *   Push to `dev` or `main` branches if `docker/Dockerfile.python`, `py/install_config/pyproject.toml`, or the workflow file itself changes.
*   **Key Jobs**:
    *   **`build`**:
        *   **Environment**: Runs on a GitHub-hosted `ubuntu-latest` runner.
        *   **Steps**:
            1.  Checks out the repository.
            2.  Sets up Docker Buildx for advanced image building.
            3.  Logs into Quay.io using robot account credentials (`secrets.QUAY_ROBOT_USERNAME`/`secrets.QUAY_ROBOT_PASSWORD`).
            4.  Builds and pushes the Docker image using `docker/build-push-action`.
                *   Uses `docker/Dockerfile.python`.
                *   Tags the image as `quay.io/semoss/python-builder:<python_version>-<compute_type>`.
                *   Passes build arguments like `PYTHON_VERSION`, `COMPUTE_TYPE`, and base image details to `Dockerfile.python`.
*   **Notes**: This workflow ensures that the Python environment required by SEMOSS is containerized with correct dependencies (from `pyproject.toml`) for both CPU and GPU compute types.

## 3. Tomcat Environment Builder (`tomcat-builder.yml`)

*   **Name**: `Tomcat Builder`
*   **Purpose**: Builds the SEMOSS Tomcat/Java/Maven base Docker image (`quay.io/semoss/tomcat-builder`). This image provides the foundational Java runtime stack for other SEMOSS images.
*   **Triggers**:
    *   Manual trigger (`workflow_dispatch`) with an input for the `version` of Tomcat to use.
    *   Push to `dev` or `main` branches if `docker/Dockerfile.tomcat` or the workflow file itself changes.
*   **Key Jobs**:
    *   **`build`**:
        *   **Environment**: Runs on a GitHub-hosted `ubuntu-latest` runner.
        *   **Steps**:
            1.  Checks out the repository.
            2.  Sets up Docker Buildx.
            3.  Logs into Quay.io.
            4.  Builds and pushes the Docker image using `docker/build-push-action`.
                *   Uses `docker/Dockerfile.tomcat`.
                *   Tags the image as `quay.io/semoss/tomcat-builder:<tomcat_version>`.
                *   Passes the `TOMCAT_VERSION` as a build argument to `Dockerfile.tomcat`.
*   **Notes**: This workflow standardizes the Java, Tomcat, and Maven versions used in SEMOSS deployments by creating a reusable base image.

## 4. SEMOSS Application Image Builders

These workflows build the final, runnable SEMOSS application images by combining the Java application, the Python environment, and other necessary components on a base OS.

### 4.1. `ubuntu2204.yml` (SEMOSS CPU Image)

*   **Name**: `Semoss Ubuntu Builder`
*   **Purpose**: Builds and pushes the main CPU-based SEMOSS application Docker image (`quay.io/semoss/semoss` or `quay.io/semoss/semoss-dev`).
*   **Triggers**:
    *   Push to `dev` or `main` if `docker/Dockerfile.ubuntu22.04` or the workflow file changes.
    *   Manual trigger (`workflow_dispatch`).
    *   Scheduled daily run (7 AM UTC).
*   **Key Jobs**:
    *   **`build`**:
        *   **Environment**: Runs on a self-hosted runner, using a `quay.io/semoss/test-quay:ubuntu-dind` (Docker-in-Docker) container to perform the build.
        *   **Steps**:
            1.  Checks out code.
            2.  Logs into Quay.io.
            3.  **Determines Image Tag**:
                *   If `vars.RELEASE == 'true'`: `quay.io/semoss/semoss:${vars.VERSION}-ubuntu22`.
                *   Else: `quay.io/semoss/semoss-dev:${vars.VERSION}-SNAPSHOT-ubuntu22-$DATE` and `quay.io/semoss/semoss-dev:${vars.VERSION}-SNAPSHOT-ubuntu22-latest`.
            4.  Builds and pushes using `docker/build-push-action` with `docker/Dockerfile.ubuntu22.04`.
            5.  Uses `no-cache: true` to ensure a fresh build.

### 4.2. `ubuntu2204_cuda.yml` (SEMOSS GPU Image)

*   **Name**: `Semoss Ubuntu-CUDA Builder`
*   **Purpose**: Builds and pushes the GPU-enabled SEMOSS application Docker image.
*   **Triggers**:
    *   Push to `dev` or `main` if `docker/Dockerfile.nvidia.cuda.12.5.1.ubuntu22.04` or the workflow file changes.
    *   Manual trigger (`workflow_dispatch`).
    *   Scheduled daily run (7:30 AM UTC).
*   **Key Jobs**:
    *   **`build`**:
        *   **Environment**: Same as the CPU image builder (self-hosted, DinD container).
        *   **Steps**: Similar to the CPU image builder, but:
            *   Uses `docker/Dockerfile.nvidia.cuda.12.5.1.ubuntu22.04`.
            *   Image tags include a `-cuda` suffix (e.g., `quay.io/semoss/semoss:${vars.VERSION}-ubuntu22-cuda`).
*   **Notes**: These application image builders ensure that deployable SEMOSS images (both CPU and GPU) are regularly built and published, incorporating the latest changes from the base builder images (Tomcat, Python) and the application source code. The use of self-hosted DinD runners provides a controlled environment for these critical builds.
```
