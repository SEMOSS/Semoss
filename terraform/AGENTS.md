# Repository Agent Guidance

- Prefer the Terraform EKS module in `terraform/modules/eks-basic` for new AWS Kubernetes deployments.
- Treat the module as a baseline for existing VPCs and subnets rather than a full networking stack.
- Ask for or confirm the AWS region, VPC ID, subnet IDs, cluster name, endpoint exposure, admin principal ARNs, node size/count, Kubernetes version, and any KMS key ARN before expanding the module.
- Keep environment-specific values in the example or caller layer, not inside the module.
- Use managed node groups, EKS access entries, and IRSA unless the use case explicitly needs something different.
- If a future change needs to mirror a CloudFormation source of truth, align the Terraform variables and resources to that source before adding extra features.