variable "cluster_name" {
  description = "Name of the EKS cluster."
  type        = string
}

variable "kubernetes_version" {
  description = "EKS Kubernetes version."
  type        = string
  default     = "1.31"
}

variable "vpc_id" {
  description = "Existing VPC ID used by the cluster."
  type        = string
}

variable "subnet_ids" {
  description = "Subnet IDs for the EKS control plane."
  type        = list(string)
}

variable "cluster_endpoint_private_access" {
  description = "Whether the cluster endpoint is reachable from within the VPC."
  type        = bool
  default     = true
}

variable "cluster_endpoint_public_access" {
  description = "Whether the cluster endpoint is publicly reachable."
  type        = bool
  default     = false
}

variable "cluster_endpoint_public_access_cidrs" {
  description = "CIDR blocks allowed to reach the public cluster endpoint."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "cluster_security_group_ingress_port" {
  description = "TCP port to allow into the EKS cluster security group from cluster_security_group_ingress_cidr_ipv4."
  type        = number
  default     = 8443

  validation {
    condition     = var.cluster_security_group_ingress_port >= 1 && var.cluster_security_group_ingress_port <= 65535
    error_message = "cluster_security_group_ingress_port must be between 1 and 65535."
  }
}

variable "cluster_security_group_ingress_cidr_ipv4" {
  description = "IPv4 CIDR allowed into the EKS cluster security group on cluster_security_group_ingress_port. When null, uses the VPC CIDR block."
  type        = string
  default     = null

  validation {
    condition     = var.cluster_security_group_ingress_cidr_ipv4 == null || can(cidrhost(var.cluster_security_group_ingress_cidr_ipv4, 0))
    error_message = "cluster_security_group_ingress_cidr_ipv4 must be a valid IPv4 CIDR block or null."
  }
}

variable "manage_cluster_security_group_ingress_rule" {
  description = "Whether this module should create the EKS cluster security group ingress rule. Set false when the rule already exists outside Terraform management."
  type        = bool
  default     = false
}

variable "cluster_log_types" {
  description = "Control plane log types to enable."
  type        = list(string)
  default     = ["api", "audit", "authenticator", "controllerManager", "scheduler"]
}

variable "cluster_admin_principal_arns" {
  description = "IAM principal ARNs that should get cluster-admin access through EKS access entries."
  type        = list(string)
  default     = []
}

variable "eks_auto_mode_node_pools" {
  description = "Node pools for EKS Auto Mode. Valid values are general-purpose and system."
  type        = list(string)
  default     = ["general-purpose"]

  validation {
    condition     = alltrue([for pool in var.eks_auto_mode_node_pools : contains(["general-purpose", "system"], pool)])
    error_message = "eks_auto_mode_node_pools values must be either general-purpose or system."
  }
}

variable "create_cluster_encryption_key" {
  description = "Whether to create a customer-managed KMS key for Kubernetes secrets encryption at rest."
  type        = bool
  default     = false
}

variable "cluster_encryption_key_arn" {
  description = "Optional existing KMS key ARN used to encrypt Kubernetes secrets at rest."
  type        = string
  default     = null

  validation {
    condition     = !(var.create_cluster_encryption_key && var.cluster_encryption_key_arn != null)
    error_message = "Set either create_cluster_encryption_key=true or cluster_encryption_key_arn, but not both."
  }
}

variable "create_bucket" {
  description = "Whether to create an S3 bucket for application file storage."
  type        = bool
  default     = true
}

variable "bucket_name" {
  description = "Optional name for the application files S3 bucket. If null, a name is generated from the cluster name with a -bucket suffix."
  type        = string
  default     = null
}

variable "bucket_kms_key_arn" {
  description = "Optional KMS key ARN for S3 bucket server-side encryption. When null, SSE-S3 (AES256) is used."
  type        = string
  default     = null
}

variable "bucket_force_destroy" {
  description = "Whether to allow deleting the application files bucket even when it contains objects."
  type        = bool
  default     = false
}

variable "cluster_iam_role_arn" {
  description = "ARN of an existing IAM role for the EKS cluster. When set, the module will not create a cluster IAM role. The provided role must already have the required EKS cluster policies attached."
  type        = string
  default     = null
}

variable "node_iam_role_arn" {
  description = "ARN of an existing IAM role for EKS Auto Mode managed instances. When set, the module will not create a node IAM role."
  type        = string
  default     = null
}

variable "tags" {
  description = "Common tags applied to all resources."
  type        = map(string)
  default     = {}
}
