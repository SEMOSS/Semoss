data "aws_partition" "current" {}

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

data "aws_vpc" "selected" {
  id = var.vpc_id
}

data "aws_iam_policy_document" "cluster_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["eks.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "node_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

locals {
  tags                      = merge({ Name = var.cluster_name }, var.tags)
  effective_encryption_key_arn           = coalesce(var.cluster_encryption_key_arn, try(aws_kms_key.cluster_encryption[0].arn, null))
  effective_cluster_sg_ingress_cidr_ipv4 = coalesce(var.cluster_security_group_ingress_cidr_ipv4, data.aws_vpc.selected.cidr_block)
  sanitized_cluster_name                 = trim(replace(replace(lower(var.cluster_name), "/[^a-z0-9-]/", "-"), "/-+/", "-"), "-")
  bucket_name                            = coalesce(var.bucket_name, "${local.sanitized_cluster_name}-bucket")
  create_cluster_role                    = var.cluster_iam_role_arn == null
  effective_cluster_role_arn             = var.cluster_iam_role_arn != null ? var.cluster_iam_role_arn : aws_iam_role.cluster[0].arn
  create_node_role                       = var.node_iam_role_arn == null
  effective_node_role_arn                = var.node_iam_role_arn != null ? var.node_iam_role_arn : aws_iam_role.node[0].arn
}

resource "aws_s3_bucket" "platform_files" {
  count = var.create_bucket ? 1 : 0

  bucket        = local.bucket_name
  force_destroy = var.bucket_force_destroy
  tags          = merge(local.tags, { Name = local.bucket_name })
}

resource "aws_s3_bucket_versioning" "platform_files" {
  count = var.create_bucket ? 1 : 0

  bucket = aws_s3_bucket.platform_files[0].id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "platform_files" {
  count = var.create_bucket ? 1 : 0

  bucket = aws_s3_bucket.platform_files[0].id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "platform_files" {
  count = var.create_bucket ? 1 : 0

  bucket = aws_s3_bucket.platform_files[0].id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = var.bucket_kms_key_arn
      sse_algorithm     = var.bucket_kms_key_arn != null ? "aws:kms" : "AES256"
    }
  }
}

resource "aws_iam_role" "cluster" {
  count = local.create_cluster_role ? 1 : 0

  name               = "${var.cluster_name}-cluster-role"
  assume_role_policy = data.aws_iam_policy_document.cluster_assume_role.json
  tags               = local.tags
}

resource "aws_iam_role_policy_attachment" "cluster" {
  count = local.create_cluster_role ? 1 : 0

  role       = aws_iam_role.cluster[0].name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonEKSClusterPolicy"
}

resource "aws_iam_role_policy_attachment" "cluster_vpc_resource_controller" {
  count = local.create_cluster_role ? 1 : 0

  role       = aws_iam_role.cluster[0].name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonEKSVPCResourceController"
}

resource "aws_iam_role_policy_attachment" "cluster_auto_compute" {
  count = local.create_cluster_role ? 1 : 0

  role       = aws_iam_role.cluster[0].name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonEKSComputePolicy"
}

resource "aws_iam_role_policy_attachment" "cluster_auto_block_storage" {
  count = local.create_cluster_role ? 1 : 0

  role       = aws_iam_role.cluster[0].name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonEKSBlockStoragePolicy"
}

resource "aws_iam_role_policy_attachment" "cluster_auto_load_balancing" {
  count = local.create_cluster_role ? 1 : 0

  role       = aws_iam_role.cluster[0].name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonEKSLoadBalancingPolicy"
}

resource "aws_iam_role_policy_attachment" "cluster_auto_networking" {
  count = local.create_cluster_role ? 1 : 0

  role       = aws_iam_role.cluster[0].name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonEKSNetworkingPolicy"
}

resource "aws_iam_role" "node" {
  count = local.create_node_role ? 1 : 0

  name               = "${var.cluster_name}-node-role"
  assume_role_policy = data.aws_iam_policy_document.node_assume_role.json
  tags               = local.tags
}

resource "aws_iam_role_policy_attachment" "node_worker_minimal" {
  count = local.create_node_role ? 1 : 0

  role       = aws_iam_role.node[0].name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonEKSWorkerNodeMinimalPolicy"
}

resource "aws_iam_role_policy_attachment" "node_ecr_pull_only" {
  count = local.create_node_role ? 1 : 0

  role       = aws_iam_role.node[0].name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonEC2ContainerRegistryPullOnly"
}

data "aws_iam_policy_document" "cluster_kms_key" {
  count = var.create_cluster_encryption_key ? 1 : 0

  statement {
    sid    = "AllowAccountAdministration"
    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = ["arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:root"]
    }

    actions   = ["kms:*"]
    resources = ["*"]
  }

  statement {
    sid    = "AllowEKSClusterRoleUsage"
    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = [local.effective_cluster_role_arn]
    }

    actions = [
      "kms:Decrypt",
      "kms:DescribeKey",
      "kms:Encrypt",
      "kms:GenerateDataKey*",
      "kms:ReEncrypt*",
    ]
    resources = ["*"]
  }
}

resource "aws_kms_key" "cluster_encryption" {
  count = var.create_cluster_encryption_key ? 1 : 0

  description         = "KMS key for EKS secrets encryption for ${var.cluster_name}"
  enable_key_rotation = true
  policy              = data.aws_iam_policy_document.cluster_kms_key[0].json
  tags                = local.tags
}

resource "aws_kms_alias" "cluster_encryption" {
  count = var.create_cluster_encryption_key ? 1 : 0

  name          = "alias/${var.cluster_name}-eks-secrets"
  target_key_id = aws_kms_key.cluster_encryption[0].key_id
}

resource "aws_eks_cluster" "this" {
  name     = var.cluster_name
  role_arn = local.effective_cluster_role_arn
  version  = var.kubernetes_version
  tags     = local.tags
  bootstrap_self_managed_addons = false

  enabled_cluster_log_types = var.cluster_log_types

  access_config {
    authentication_mode                         = "API_AND_CONFIG_MAP"
    bootstrap_cluster_creator_admin_permissions = false
  }

  vpc_config {
    subnet_ids              = var.subnet_ids
    endpoint_private_access = var.cluster_endpoint_private_access
    endpoint_public_access  = var.cluster_endpoint_public_access
    public_access_cidrs     = var.cluster_endpoint_public_access ? var.cluster_endpoint_public_access_cidrs : null
  }

  compute_config {
    enabled       = true
    node_pools    = var.eks_auto_mode_node_pools
    node_role_arn = local.effective_node_role_arn
  }

  kubernetes_network_config {
    elastic_load_balancing {
      enabled = true
    }
  }

  storage_config {
    block_storage {
      enabled = true
    }
  }

  dynamic "encryption_config" {
    for_each = local.effective_encryption_key_arn == null ? [] : [local.effective_encryption_key_arn]

    content {
      resources = ["secrets"]

      provider {
        key_arn = encryption_config.value
      }
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.cluster,
    aws_iam_role_policy_attachment.cluster_vpc_resource_controller,
    aws_iam_role_policy_attachment.cluster_auto_compute,
    aws_iam_role_policy_attachment.cluster_auto_block_storage,
    aws_iam_role_policy_attachment.cluster_auto_load_balancing,
    aws_iam_role_policy_attachment.cluster_auto_networking,
    aws_kms_alias.cluster_encryption,
  ]
}

resource "aws_vpc_security_group_ingress_rule" "cluster_api_from_cidr" {
  count = var.manage_cluster_security_group_ingress_rule ? 1 : 0

  security_group_id = aws_eks_cluster.this.vpc_config[0].cluster_security_group_id
  description       = "Allow TCP ${var.cluster_security_group_ingress_port} from configured CIDR"
  cidr_ipv4         = local.effective_cluster_sg_ingress_cidr_ipv4
  from_port         = var.cluster_security_group_ingress_port
  ip_protocol       = "tcp"
  to_port           = var.cluster_security_group_ingress_port
}


resource "aws_eks_access_entry" "admins" {
  for_each = toset(var.cluster_admin_principal_arns)

  cluster_name  = aws_eks_cluster.this.name
  principal_arn = each.value
  type          = "STANDARD"
}

resource "aws_eks_access_policy_association" "admins" {
  for_each = toset(var.cluster_admin_principal_arns)

  cluster_name  = aws_eks_cluster.this.name
  principal_arn = each.value
  policy_arn    = "arn:${data.aws_partition.current.partition}:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"

  access_scope {
    type = "cluster"
  }

  depends_on = [aws_eks_access_entry.admins]
}
