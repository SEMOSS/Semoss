variable "aws_region" {
  description = "AWS region where the EKS cluster will be created."
  type        = string
}

variable "cluster_name" {
  description = "Name of the EKS cluster."
  type        = string
}

variable "kubernetes_version" {
  description = "Kubernetes version for the cluster."
  type        = string
  default     = "1.31"
}

variable "vpc_id" {
  description = "Existing VPC ID."
  type        = string
}

variable "subnet_ids" {
  description = "Subnet IDs used by the cluster."
  type        = list(string)
}

variable "cluster_admin_principal_arns" {
  description = "IAM principal ARNs that should receive cluster-admin access."
  type        = list(string)
  default     = []
}

variable "cluster_endpoint_private_access" {
  description = "Whether the cluster endpoint is private."
  type        = bool
  default     = true
}

variable "cluster_endpoint_public_access" {
  description = "Whether the cluster endpoint is public."
  type        = bool
  default     = false
}

variable "cluster_endpoint_public_access_cidrs" {
  description = "CIDR blocks allowed to reach the public endpoint."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "eks_auto_mode_node_pools" {
  description = "Node pools for EKS Auto Mode."
  type        = list(string)
  default     = ["general-purpose"]
}

variable "create_cluster_encryption_key" {
  description = "Whether to create a customer-managed KMS key for EKS secrets encryption."
  type        = bool
  default     = false
}

variable "cluster_encryption_key_arn" {
  description = "Optional existing KMS key ARN for EKS secrets encryption."
  type        = string
  default     = null
}

variable "tags" {
  description = "Common tags to apply to module resources."
  type        = map(string)
  default     = {}
}