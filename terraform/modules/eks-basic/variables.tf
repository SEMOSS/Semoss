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
  description = "Managed EKS add-ons to install (CoreDNS is intentionally excluded by this module)."
  type        = set(string)
  default     = ["kube-proxy", "vpc-cni", "eks-pod-identity-agent"]
}

variable "enable_cluster_addons" {
  description = "Whether managed EKS add-ons should be installed."
  type        = bool
  default     = true
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
  description = "Optional name for the application files S3 bucket. If null, a name is generated from cluster name, account ID, and region."
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

variable "node_ami_image_id" {
  description = "Optional custom AMI ID for managed node group instances. When set, the node group uses a launch template with this AMI and ignores node_ami_type."
  type        = string
  default     = null

  validation {
    condition     = var.node_ami_image_id == null || can(regex("^ami-[0-9a-fA-F]+$", var.node_ami_image_id))
    error_message = "node_ami_image_id must be a valid AMI ID (for example, ami-0123456789abcdef0) or null."
  }
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

variable "cluster_iam_role_arn" {
  description = "ARN of an existing IAM role for the EKS cluster. When set, the module will not create a cluster IAM role. The provided role must already have the AmazonEKSClusterPolicy and AmazonEKSVPCResourceController policies attached."
  type        = string
  default     = null
}

variable "node_iam_role_arn" {
  description = "ARN of an existing IAM role to attach to worker nodes. When set, the module will not create a node IAM role and will not attach any managed policies to it. The provided role must already have the permissions required by your workloads."
  type        = string
  default     = null
}

variable "attach_ssm_policy_to_nodes" {
  description = "Whether to attach the SSM managed instance policy to worker nodes. Has no effect when node_iam_role_arn is provided."
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