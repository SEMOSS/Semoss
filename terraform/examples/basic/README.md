# Basic EKS Auto Mode example

This example consumes the `eks-basic` module from a separate Terraform root.

## Usage

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

  eks_auto_mode_node_pools      = var.eks_auto_mode_node_pools
  create_cluster_encryption_key = var.create_cluster_encryption_key
  cluster_encryption_key_arn    = var.cluster_encryption_key_arn

  tags = var.tags
}
```

## Local tfvars setup

1. Copy `terraform.tfvars.example` to `terraform.tfvars`.
2. Set VPC, subnet IDs, and optional admin principal ARNs.
3. Run `terraform init`, `terraform plan`, and `terraform apply`.
