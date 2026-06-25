# eks-basic Terraform module

This module creates a basic Amazon EKS cluster on top of an existing VPC.

It includes:

- An EKS control plane and a managed node group
- Core EKS add-ons (excluding CoreDNS)
- IAM roles and policy attachments for the cluster and nodes (with options to use existing roles)
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
- Optional existing cluster IAM role ARN if you want to use a pre-created role
- Optional existing node IAM role ARN if you want to use a pre-created role
- Optional decision on whether worker nodes should have SSM access
- Optional custom node AMI ID if you want worker nodes to use a specific image
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

## Using Existing IAM Roles

If you already have cluster and/or node IAM roles, you can provide them to avoid duplication:

```hcl
module "eks" {
  source = "../../modules/eks-basic"

  cluster_name                = "semoss-prod"
  kubernetes_version          = "1.31"
  vpc_id                      = var.vpc_id
  subnet_ids                  = var.subnet_ids
  
  # Use existing cluster IAM role
  cluster_iam_role_arn = aws_iam_role.existing_cluster_role.arn
  
  # Use existing node IAM role
  node_iam_role_arn = aws_iam_role.existing_node_role.arn

  node_instance_types = ["m6i.large"]
  node_desired_size   = 2
  node_min_size       = 2
  node_max_size       = 4

  tags = {
    application = "semoss"
    environment = "prod"
  }
}
```

**Note:** When providing existing IAM roles, they must already have the required policies attached:
- **Cluster role** must have: `AmazonEKSClusterPolicy` and `AmazonEKSVPCResourceController`
- **Node role** must have: `AmazonEKSWorkerNodePolicy`, `AmazonEKS_CNI_Policy`, and `AmazonEC2ContainerRegistryReadOnly`

## Using a Custom Node AMI

Set `node_ami_image_id` to have the node group use your AMI through a launch template:

```hcl
module "eks" {
  source = "../../modules/eks-basic"

  cluster_name       = "semoss-dev"
  kubernetes_version = "1.31"
  vpc_id             = var.vpc_id
  subnet_ids         = var.subnet_ids

  node_ami_image_id  = "ami-0123456789abcdef0"
  node_instance_types = ["m6i.large"]
  node_desired_size   = 2
  node_min_size       = 2
  node_max_size       = 4
}
```

When `node_ami_image_id` is set, the module ignores `node_ami_type`.

## Inputs

## CoreDNS Add-on Ordering

To avoid add-on and node-group dependency cycles, this module does not manage the `coredns` add-on.
Create CoreDNS in your downstream stack after node group creation.

See `variables.tf` for the full list of inputs.

## Outputs

See `outputs.tf` for the full list of outputs.