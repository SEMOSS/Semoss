# Basic EKS example

This example shows how to consume the `eks-basic` module from a separate Terraform root.

## Preflight Checklist

Before you apply this example, make sure you have:

- An AWS account, region, and authenticated Terraform session
- An existing VPC and at least two subnets for the cluster and node group
- IAM permissions to create and manage EKS, IAM, EC2, and OIDC resources
- A decision on cluster endpoint access: private, public, or both
- Optional admin principal ARNs if you want EKS access entries created
- Optional KMS key ARN if you want Kubernetes secrets encrypted at rest
- Optional decision on whether worker nodes should have SSM access
- Terraform v1.5+ and AWS provider v5+ in the consuming repo

## Usage

The module source in this repository example is local:

```hcl
module "eks" {
  source = "../../modules/eks-basic"

  cluster_name                 = var.cluster_name
  kubernetes_version           = var.kubernetes_version
  vpc_id                       = var.vpc_id
  subnet_ids                   = var.subnet_ids
  cluster_admin_principal_arns = var.cluster_admin_principal_arns

  cluster_endpoint_private_access      = var.cluster_endpoint_private_access
  cluster_endpoint_public_access       = var.cluster_endpoint_public_access
  cluster_endpoint_public_access_cidrs = var.cluster_endpoint_public_access_cidrs

  node_instance_types = var.node_instance_types
  node_desired_size   = var.node_desired_size
  node_min_size       = var.node_min_size
  node_max_size       = var.node_max_size

  tags = var.tags
}
```

If you are using this module from another repo, replace the local source with a Git source such as `git::https://github.com/<org>/semoss.git//terraform/modules/eks-basic?ref=v1.0.0`.

## Variables

See `variables.tf` for the full set of example inputs.

## Local tfvars setup

Use the provided sample input file:

1. Copy `terraform.tfvars.example` to `terraform.tfvars` in this same folder.
2. Update the values for your VPC, subnets, IAM principals, and sizing.
3. Run `terraform init`, `terraform plan`, and `terraform apply` from this folder.