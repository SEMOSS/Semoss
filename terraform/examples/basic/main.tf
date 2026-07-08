terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

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
  eks_auto_mode_node_pools             = var.eks_auto_mode_node_pools
  create_cluster_encryption_key        = var.create_cluster_encryption_key
  cluster_encryption_key_arn           = var.cluster_encryption_key_arn

  tags = var.tags
}