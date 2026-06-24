# eks-basic Terraform module

This module creates a basic Amazon EKS cluster on top of an existing VPC.

It includes:

- An EKS control plane and a managed node group
- Core EKS add-ons
- IAM roles and policy attachments for the cluster and nodes
- Optional EKS access entries for cluster admins
- Optional IRSA/OIDC support for Kubernetes service accounts
- Optional secrets encryption with a customer-managed KMS key

## Assumptions

- You already have a VPC and a set of subnets ready for EKS.
- You want a reusable baseline rather than a fully opinionated platform module.
- If you enable public access, you should narrow the allowed CIDRs.

## Preflight Checklist

Before you apply this module, make sure you have:

- An AWS account, region, and authenticated Terraform session
- An existing VPC and at least two subnets for the cluster and node group
- IAM permissions to create and manage EKS, IAM, EC2, and OIDC resources
- A decision on cluster endpoint access: private, public, or both
- Optional admin principal ARNs if you want EKS access entries created
- Optional KMS key ARN if you want Kubernetes secrets encrypted at rest
- Optional decision on whether worker nodes should have SSM access
- Terraform v1.5+ and AWS provider v5+ in the consuming repo

## Example

```hcl
module "eks" {
  source = "../../modules/eks-basic"

  cluster_name                 = "semoss-dev"
  kubernetes_version           = "1.31"
  vpc_id                       = var.vpc_id
  subnet_ids                   = var.subnet_ids
  cluster_admin_principal_arns = var.cluster_admin_principal_arns

  cluster_endpoint_private_access = true
  cluster_endpoint_public_access   = false

  node_instance_types = ["m6i.large"]
  node_desired_size   = 2
  node_min_size       = 2
  node_max_size       = 4

  tags = {
    application = "semoss"
    environment = "dev"
  }
}
```

## Inputs

See `variables.tf` for the full list of inputs.

## Outputs

See `outputs.tf` for the full list of outputs.