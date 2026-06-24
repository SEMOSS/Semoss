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

variable "node_instance_types" {
  description = "Instance types for the managed node group."
  type        = list(string)
  default     = ["m6i.large"]
}

variable "node_desired_size" {
  description = "Desired node count."
  type        = number
  default     = 2
}

variable "node_min_size" {
  description = "Minimum node count."
  type        = number
  default     = 2
}

variable "node_max_size" {
  description = "Maximum node count."
  type        = number
  default     = 4
}

variable "tags" {
  description = "Common tags to apply to module resources."
  type        = map(string)
  default     = {}
}