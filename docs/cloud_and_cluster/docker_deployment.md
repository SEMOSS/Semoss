# Docker Deployment for SEMOSS

SEMOSS utilizes Docker to provide containerized environments for consistent deployment and to manage its various components, including the main Java web application, Python execution environments, and specialized GPU-enabled setups. This document details the key Dockerfiles found in the `docker/` directory.

## Introduction

Using Docker for SEMOSS offers several advantages:
*   **Consistency**: Ensures that the SEMOSS runtime environment is the same across development, testing, and production.
*   **Dependency Management**: Packages all necessary dependencies (Java, Tomcat, Python, R, system libraries) within the image.
*   **Scalability**: Facilitates scaling SEMOSS instances, especially when used with container orchestration platforms like Kubernetes.
*   **Isolation**: Provides process isolation for different components.

## Core Dockerfiles

### 1. `Dockerfile.ubuntu22.04` (SEMOSS Base Image)

*   **Purpose**: This is a comprehensive base image for running the main SEMOSS application. It bundles the Java web application (Tomcat), a Python environment, Maven, and necessary system utilities on an Ubuntu 22.04 base.
*   **Base Image**: `ubuntu:22.04`
*   **Key Build Stages & Components**:
    *   Uses a multi-stage build.
    *   **Java/Tomcat/Maven**: Copies pre-built Java (Zulu JDK 8), Tomcat (e.g., 9.0.104), and Maven (e.g., 3.8.5) from a `quay.io/semoss/tomcat-builder` image (detailed separately).
    *   **SEMOSS Application**:
        *   Clones the `semoss-artifacts` repository from GitHub.
        *   Runs an `update_latest_dev.sh` script, which is responsible for fetching or setting up the SEMOSS application files (likely into `/opt/semosshome/`). This includes web application files for Tomcat, configuration files, etc.
        *   Modifies Tomcat's `catalina.properties` to optimize JAR scanning.
    *   **Python Environment**:
        *   Copies a pre-built Python environment from `quay.io/semoss/python-builder:3.12-cpu` into `/usr/lib/python/`. This includes a virtual environment at `/usr/lib/python/semossvenv`.
    *   **System Utilities**: Installs `git`, `curl`, `unzip`, `lsof`, `vim`, `nano`, `rclone`, `locales`, `debootstrap`, `fakechroot`, `fakeroot`.
    *   **User**: Creates and runs as a non-root user `default` (UID 1001).
    *   A `/opt/chroot` directory is created, potentially for use by chrooted Python processes.
*   **Key Environment Variables**:
    *   `JAVA_HOME`: Set to the path of Zulu JDK 8 (e.g., `/usr/lib/jvm/zulu8`).
    *   `TOMCAT_HOME`: Path to the Tomcat installation (e.g., `/opt/apache-tomcat-9.0.104`).
    *   `MAVEN_HOME`: Path to the Maven installation.
    *   `VIRTUAL_ENV`: Path to the Python virtual environment (`/usr/lib/python/semossvenv`).
    *   `PATH`: Updated to include binaries from JDK, Tomcat, Maven, Python venv, and `semoss-artifacts` scripts.
    *   Locale variables (`LANG`, `LANGUAGE`, `LC_ALL`) are set to `en_US.UTF-8`.
*   **Exposed Ports**: While not explicitly defined with `EXPOSE`, Tomcat (started by the CMD) will listen on the port configured in its `server.xml` (typically 8080). This port needs to be published during `docker run`.
*   **Volume Mounts (Recommended)**:
    *   `/opt/semosshome/db`: To persist internal SEMOSS databases.
    *   `/opt/semosshome/InsightCache`: To persist insight cache.
    *   `/opt/semosshome/logs`: To persist SEMOSS application logs from Tomcat.
    *   `/opt/semosshome/project`, `/opt/semosshome/model`, `/opt/semosshome/vector`, etc.: To persist user-created assets if not using centralized cloud storage.
    *   `/opt/semosshome/rdf_map/`: To provide custom `RDF_Map.prop`, `social.properties`, `config.properties`.
    *   `/opt/chroot/`: If specific chrooted Python environments require persistent storage or shared data.
*   **Default Command**: `exec $TOMCAT_HOME/bin/start.sh` (Starts Tomcat and tails `catalina.out`).
*   **Example Usage**:
    ```bash
    # Build (if building locally, typically these are pulled from a registry)
    # docker build -t semoss/base:ubuntu22.04 -f docker/Dockerfile.ubuntu22.04 .

    # Run
    docker run -d -p 8080:8080 \
        -v /path/on/host/semoss_home/db:/opt/semosshome/db \
        -v /path/on/host/semoss_home/InsightCache:/opt/semosshome/InsightCache \
        -v /path/on/host/semoss_home/logs:/opt/semosshome/logs \
        -v /path/on/host/semoss_home/rdf_map:/opt/semosshome/rdf_map \
        --name semoss-instance \
        quay.io/semoss/semoss:latest
        # (Assuming quay.io/semoss/semoss:latest is built using this Dockerfile or similar)
    ```

### 2. `Dockerfile.nvidia.cuda.12.5.1.ubuntu22.04` (SEMOSS GPU Image)

*   **Purpose**: Builds a SEMOSS image with NVIDIA GPU support, based on an official NVIDIA CUDA runtime image. This is for running GPU-accelerated machine learning tasks.
*   **Base Image**: `nvidia/cuda:12.5.1-runtime-ubuntu22.04`
*   **Key Differences from `Dockerfile.ubuntu22.04`**:
    *   Uses the NVIDIA CUDA image as its foundation for both the `mavenpuller` and `final` stages.
    *   Copies the Python environment from a GPU-specific builder: `COPY --from=quay.io/semoss/python-builder:3.12-gpu /usr/lib/python /usr/lib/python`. This version of the Python environment includes libraries compiled with GPU support (e.g., PyTorch with CUDA).
*   **Other Aspects**: Most other build steps, environment variables, recommended volumes, and the default command are identical to `Dockerfile.ubuntu22.04`.
*   **Example Usage (requires NVIDIA Docker runtime)**:
    ```bash
    # Run (ensure nvidia-container-toolkit is installed on the host)
    docker run -d -p 8080:8080 --gpus all \
        -v /path/on/host/semoss_home/db:/opt/semosshome/db \
        # ... other volume mounts ...
        --name semoss-gpu-instance \
        quay.io/semoss/semoss-gpu:latest
        # (Assuming quay.io/semoss/semoss-gpu:latest is built with this Dockerfile)
    ```

### 3. `Dockerfile.tomcat` (Tomcat/Java/Maven Builder Base)

*   **Purpose**: This Dockerfile creates a base image (`quay.io/semoss/tomcat-builder`) that bundles specific versions of Java (Zulu JDK 8), Apache Tomcat, and Apache Maven. It's used as a source for these components in `Dockerfile.ubuntu22.04` and `Dockerfile.nvidia.cuda...` to ensure consistent versions.
*   **Base Image**: `ubuntu:22.04`
*   **Key Build Stages & Components**:
    *   Downloads and installs specified versions of Zulu OpenJDK, Apache Tomcat, and Apache Maven.
    *   Clones `semoss-artifacts` to get scripts (`install_java.sh`, `config.sh`) and Tomcat configuration files (`server.xml`, `web.xml`) which are copied into the Tomcat installation.
    *   Creates custom `start.sh` and `stop.sh` scripts for Tomcat.
*   **Final Image**: The `final` stage starts `FROM scratch` and copies the entire filesystem from the `builder` stage, resulting in an image containing just the installed JDK, Tomcat, and Maven.
*   **Default Command**: `start.sh` (which starts Tomcat).
*   **Note**: This image is primarily a builder/source image, not typically run directly by end-users for SEMOSS application deployment.

### 4. `Dockerfile.python` (Python Environment Builder Base)

*   **Purpose**: This Dockerfile creates a Python environment with dependencies specified in `py/install_config/pyproject.toml`. It produces images like `quay.io/semoss/python-builder:3.12-cpu` and `quay.io/semoss/python-builder:3.12-gpu` which are then used in the main SEMOSS Dockerfiles.
*   **Base Image**: A specified Ubuntu version (e.g., `ubuntu:22.04`).
*   **Key Build Stages & Components**:
    *   Installs build tools, `curl`, `clang`, and Rust.
    *   Installs `uv` (Python package installer).
    *   Installs a specified Python version using `uv python install`.
    *   Creates a virtual environment at `/usr/lib/python/semossvenv`.
    *   Downloads `pyproject.toml` from the SEMOSS GitHub repository.
    *   Installs Python dependencies using `uv pip install -r pyproject.toml --extra <COMPUTE_TYPE>`, where `COMPUTE_TYPE` (e.g., "cpu", "gpu") is a build argument.
    *   The `final` stage copies the created Python environment (`/usr/lib/python`) and also installs `tesseract-ocr`.
*   **Default Command**: `["bash"]`. This image is primarily for providing a Python environment to be copied into other images.
*   **Note**: This image is a builder/source image.

### 5. `Dockerfile.dind` (Docker-in-Docker)

*   **Purpose**: Creates an Ubuntu-based image with Docker Engine, CLI, and Compose installed. This allows running Docker commands from within a Docker container (Docker-in-Docker).
*   **Base Image**: `ubuntu:22.04`
*   **Key Build Stages & Components**:
    *   Installs prerequisites like `curl`, `ca-certificates`.
    *   Adds Docker's official GPG key and APT repository.
    *   Installs `docker-ce`, `docker-ce-cli`, `containerd.io`, `docker-buildx-plugin`, `docker-compose-plugin`.
*   **Default Command**: `["bash"]`.
*   **Usage Context**: Likely used for CI/CD pipelines or development/testing scenarios within the SEMOSS project that require building or interacting with Docker containers.
*   **Example Run (interacting with host's Docker daemon)**:
    ```bash
    docker run -v /var/run/docker.sock:/var/run/docker.sock -ti your-dind-image-name bash
    ```
    *(Note: Accessing the host's Docker socket has security implications and should be done with caution.)*

This set of Dockerfiles provides a flexible way to build and deploy SEMOSS in various configurations, from standard CPU-based deployments to GPU-accelerated environments, and includes utilities for development and CI/CD.
