# SEMOSS in Cloud and Clustered Environments

As SEMOSS deployments scale to handle more users, larger datasets, and higher availability requirements, running it in a cloud or clustered environment becomes essential. This section of the documentation details the architecture and components that enable SEMOSS to operate effectively in such distributed setups, particularly when deployed using containerization technologies like Docker and orchestration platforms like Kubernetes.

## Why a Distributed Architecture?

Deploying SEMOSS in a clustered or cloud-native fashion addresses several key needs:

*   **Scalability**:
    *   **Horizontal Scaling**: Multiple SEMOSS instances can run concurrently to distribute user load, improving responsiveness and throughput for analytical queries, Pixel executions, and API requests.
    *   **Resource Intensive Tasks**: Computationally heavy tasks (e.g., complex data processing, model training/inference, large data imports/exports) can be managed more effectively by distributing them or by scaling specific components.
*   **High Availability (HA)**:
    *   Running multiple instances of SEMOSS ensures that if one instance fails, others can continue to serve user requests, minimizing downtime.
    *   Load balancers can distribute traffic across healthy instances.
*   **Shared Access to Assets**:
    *   In a distributed environment, users connected to different SEMOSS instances need consistent access to shared assets like projects, data engines (databases, models, etc.), and insights. A centralized storage mechanism is required for these assets.
*   **Statelessness and Containerization**:
    *   Modern cloud deployments often favor stateless application tiers. While SEMOSS has stateful components (like in-memory data frames within an `Insight`), the architecture aims to manage shared state (project/engine configurations, user data) in a way that supports running instances in containers.
*   **Efficient Resource Utilization**: Cloud platforms allow for dynamic scaling of resources based on demand, which can be more cost-effective than maintaining large, monolithic on-premises servers.

## Key Components for Cloud/Cluster Operation

To achieve these goals, the SEMOSS backend incorporates several key components and strategies when running in a distributed mode:

1.  **Centralized Asset Storage**:
    *   A shared repository, typically leveraging cloud storage services (like AWS S3, Azure Blob Storage, Google Cloud Storage) or a network file system accessible to all instances.
    *   SEMOSS projects, engine configurations (`.smss` files), cached data, and other critical assets are stored centrally.
    *   The `prerna.cluster.util.CentralCloudStorage.java` class plays a key role in managing the push and pull of these assets to/from the central store.
    *   This ensures that all SEMOSS instances in the cluster work with a consistent set of application data and configurations.

2.  **Cluster Synchronization (ZooKeeper)**:
    *   Apache ZooKeeper is used for coordination and synchronization among the different SEMOSS instances in the cluster.
    *   The `prerna.cluster.util.ClusterUtilZkSynchronizer.java` class handles interactions with ZooKeeper.
    *   **Responsibilities include**:
        *   Notifying instances of changes to shared assets (e.g., when a new engine is added or an existing one is updated via one instance, other instances are informed so they can refresh their local state/cache).
        *   Managing distributed locks or coordinating operations that should only be performed by one instance at a time (e.g., certain types of cache updates or schema modifications).
        *   Potentially, service discovery or leader election in some scenarios.

3.  **Cluster Utilities (`prerna.cluster.util`)**:
    *   This package contains various helper classes that support clustered operations, including `CentralCloudStorage` and `ClusterUtilZkSynchronizer`.
    *   Other utilities might assist with tasks like inter-instance communication (if any direct communication is used beyond ZooKeeper) or managing cluster-wide configurations.

4.  **Terraform EKS Module**:
    *   Reusable baseline module for deploying a basic EKS cluster from Terraform: [terraform/modules/eks-basic/README.md](../../terraform/modules/eks-basic/README.md)

These components work together to allow multiple SEMOSS instances to operate as a cohesive unit, providing a scalable, resilient, and consistent user experience.

## Detailed Documentation

The following documents delve into the specifics of these components and related deployment strategies:

1.  **[Centralized Cloud Storage for Assets](./central_cloud_storage.md)**
2.  **[Cluster Synchronization with ZooKeeper](./cluster_synchronization.md)**
3.  **[Docker Deployment Guide](./docker_deployment.md)**
4.  **[GitHub Actions Workflows for CI/CD](./github_actions_workflows.md)**
    *   Overview of CI, Python/Tomcat Builders, Application Image Builders.
