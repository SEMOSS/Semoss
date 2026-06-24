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
  description = "Subnet IDs for the EKS control plane and node group."
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

variable "cluster_addon_names" {
  description = "Managed EKS add-ons to install."
  type        = set(string)
  default     = ["coredns", "kube-proxy", "vpc-cni", "eks-pod-identity-agent"]
}

variable "enable_cluster_addons" {
  description = "Whether managed EKS add-ons should be installed."
  type        = bool
  default     = true
}

variable "cluster_encryption_key_arn" {
  description = "Optional KMS key ARN used to encrypt Kubernetes secrets at rest."
  type        = string
  default     = null
}

variable "node_group_name" {
  description = "Name of the managed node group."
  type        = string
  default     = "default"
}

variable "node_instance_types" {
  description = "Instance types for the managed node group."
  type        = list(string)
  default     = ["m6i.large"]
}

variable "node_capacity_type" {
  description = "Capacity type for the managed node group."
  type        = string
  default     = "ON_DEMAND"

  validation {
    condition     = contains(["ON_DEMAND", "SPOT"], var.node_capacity_type)
    error_message = "node_capacity_type must be ON_DEMAND or SPOT."
  }
}

variable "node_ami_type" {
  description = "AMI type for the managed node group."
  type        = string
  default     = "AL2_x86_64"
}

variable "node_disk_size" {
  description = "Root disk size in GiB for worker nodes."
  type        = number
  default     = 50
}

variable "node_desired_size" {
  description = "Desired number of nodes in the managed node group."
  type        = number
  default     = 2
}

variable "node_min_size" {
  description = "Minimum number of nodes in the managed node group."
  type        = number
  default     = 2
}

variable "node_max_size" {
  description = "Maximum number of nodes in the managed node group."
  type        = number
  default     = 4

  validation {
    condition     = var.node_min_size <= var.node_desired_size && var.node_desired_size <= var.node_max_size
    error_message = "node_min_size must be <= node_desired_size and node_desired_size must be <= node_max_size."
  }
}

variable "node_labels" {
  description = "Labels applied to the managed node group."
  type        = map(string)
  default     = {}
}

variable "node_taints" {
  description = "Taints applied to the managed node group."
  type = list(object({
    key    = string
    value  = string
    effect = string
  }))
  default = []
}

variable "node_group_tags" {
  description = "Additional tags for the managed node group resource."
  type        = map(string)
  default     = {}
}

variable "attach_ssm_policy_to_nodes" {
  description = "Whether to attach the SSM managed instance policy to worker nodes."
  type        = bool
  default     = false
}

variable "enable_irsa" {
  description = "Whether to create an OIDC provider for IAM roles for service accounts."
  type        = bool
  default     = true
}

variable "tags" {
  description = "Common tags applied to all resources."
  type        = map(string)
  default     = {}
}