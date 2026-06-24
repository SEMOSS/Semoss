output "cluster_name" {
  description = "EKS cluster name."
  value       = aws_eks_cluster.this.name
}

output "cluster_arn" {
  description = "EKS cluster ARN."
  value       = aws_eks_cluster.this.arn
}

output "cluster_endpoint" {
  description = "API server endpoint for the cluster."
  value       = aws_eks_cluster.this.endpoint
}

output "cluster_certificate_authority_data" {
  description = "Base64-encoded certificate authority data for the cluster."
  value       = aws_eks_cluster.this.certificate_authority[0].data
}

output "cluster_security_group_id" {
  description = "Security group created by EKS for the cluster control plane."
  value       = aws_eks_cluster.this.vpc_config[0].cluster_security_group_id
}

output "cluster_oidc_issuer_url" {
  description = "OIDC issuer URL for the cluster."
  value       = aws_eks_cluster.this.identity[0].oidc[0].issuer
}

output "cluster_oidc_provider_arn" {
  description = "ARN of the IAM OIDC provider if IRSA is enabled."
  value       = try(aws_iam_openid_connect_provider.this[0].arn, null)
}

output "node_group_arn" {
  description = "Managed node group ARN."
  value       = aws_eks_node_group.this.arn
}

output "node_group_status" {
  description = "Managed node group status."
  value       = aws_eks_node_group.this.status
}

output "node_role_arn" {
  description = "IAM role ARN used by worker nodes."
  value       = aws_iam_role.node.arn
}