# eks-basic Terraform module

This module creates an Amazon EKS cluster on top of an existing VPC using EKS Auto Mode.

It includes:

- EKS control plane with Auto Mode enabled
- Auto Mode compute, load balancing, and block storage capabilities
- IAM roles and policy attachments for cluster and Auto Mode managed instances (with options to use existing roles)
- Optional EKS access entries for cluster admins
- Optional secrets encryption with a customer-managed KMS key
- Optional S3 bucket for application file storage

## Assumptions

- You already have a VPC and subnets ready for EKS.
- You want EKS Auto Mode as the cluster operation model.
- If you enable public endpoint access, you should restrict CIDRs.

## Example

```hcl
module "eks" {
  source = "../../modules/eks-basic"

  cluster_name                 = "semoss-dev"
  kubernetes_version           = "1.31"
  vpc_id                       = var.vpc_id
  subnet_ids                   = var.subnet_ids
  cluster_admin_principal_arns = var.cluster_admin_principal_arns

  cluster_endpoint_private_access      = true
  cluster_endpoint_public_access       = false
  cluster_endpoint_public_access_cidrs = ["10.0.0.0/8"]

  eks_auto_mode_node_pools = ["general-purpose"]

  tags = {
    application = "semoss"
    environment = "dev"
  }
}
```

## Auto Mode behavior

This module configures:

- `bootstrap_self_managed_addons = false`
- `compute_config.enabled = true`
- `compute_config.node_role_arn` from `node_iam_role_arn` or module-created node role
- `compute_config.node_pools` from `eks_auto_mode_node_pools`
- `kubernetes_network_config.elastic_load_balancing.enabled = true`
- `storage_config.block_storage.enabled = true`

## IAM requirements

If you provide existing IAM roles:

- Cluster role must include EKS cluster permissions and Auto Mode control-plane permissions.
- Node role must include Auto Mode worker permissions.

If you let the module create roles, it attaches:

- Cluster role: `AmazonEKSClusterPolicy`, `AmazonEKSVPCResourceController`, `AmazonEKSComputePolicy`, `AmazonEKSBlockStoragePolicy`, `AmazonEKSLoadBalancingPolicy`, `AmazonEKSNetworkingPolicy`
- Node role: `AmazonEKSWorkerNodeMinimalPolicy`, `AmazonEC2ContainerRegistryPullOnly`

## Inputs and Outputs

See `variables.tf` and `outputs.tf` for the complete interface.
