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
  tags                         = merge({ Name = var.cluster_name }, var.tags)
  addon_names                  = var.enable_cluster_addons ? toset([for addon in var.cluster_addon_names : addon if lower(addon) != "coredns"]) : toset([])
  node_all_tags                = merge(local.tags, var.node_group_tags)
  effective_encryption_key_arn = coalesce(var.cluster_encryption_key_arn, try(aws_kms_key.cluster_encryption[0].arn, null))
  bucket_name_base             = coalesce(var.bucket_name, "${lower(replace(var.cluster_name, "/[^a-z0-9-]/", "-"))}-app-files-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.name}")
  bucket_name                  = trim(local.bucket_name_base, "-")
  create_cluster_role          = var.cluster_iam_role_arn == null
  effective_cluster_role_arn   = var.cluster_iam_role_arn != null ? var.cluster_iam_role_arn : aws_iam_role.cluster[0].arn
  create_node_role             = var.node_iam_role_arn == null
  effective_node_role_arn      = var.node_iam_role_arn != null ? var.node_iam_role_arn : aws_iam_role.node[0].arn
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

resource "aws_iam_role" "node" {
  count = local.create_node_role ? 1 : 0

  name               = "${var.cluster_name}-node-role"
  assume_role_policy = data.aws_iam_policy_document.node_assume_role.json
  tags               = local.tags
}

resource "aws_iam_role_policy_attachment" "node_worker" {
  count = local.create_node_role ? 1 : 0

  role       = aws_iam_role.node[0].name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonEKSWorkerNodePolicy"
}

resource "aws_iam_role_policy_attachment" "node_cni" {
  count = local.create_node_role ? 1 : 0

  role       = aws_iam_role.node[0].name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonEKS_CNI_Policy"
}

resource "aws_iam_role_policy_attachment" "node_ecr_readonly" {
  count = local.create_node_role ? 1 : 0

  role       = aws_iam_role.node[0].name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}

resource "aws_iam_role_policy_attachment" "node_ssm" {
  count = (local.create_node_role && var.attach_ssm_policy_to_nodes) ? 1 : 0

  role       = aws_iam_role.node[0].name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonSSMManagedInstanceCore"
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
    aws_kms_alias.cluster_encryption,
  ]
}

resource "aws_eks_addon" "managed" {
  for_each = local.addon_names

  cluster_name                = aws_eks_cluster.this.name
  addon_name                  = each.value
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"
  tags                        = local.tags
}

resource "aws_launch_template" "node_custom_ami" {
  count = var.node_ami_image_id == null ? 0 : 1

  name_prefix = "${var.cluster_name}-${var.node_group_name}-"
  image_id    = var.node_ami_image_id

  tag_specifications {
    resource_type = "instance"
    tags          = local.node_all_tags
  }

  tags = local.node_all_tags
}

resource "aws_eks_node_group" "this" {
  cluster_name         = aws_eks_cluster.this.name
  node_group_name      = var.node_group_name
  node_role_arn        = local.effective_node_role_arn
  subnet_ids           = var.subnet_ids
  capacity_type        = var.node_capacity_type
  disk_size            = var.node_ami_image_id == null ? var.node_disk_size : null
  instance_types       = var.node_instance_types
  ami_type             = var.node_ami_image_id == null ? var.node_ami_type : null
  force_update_version = true
  tags                 = local.node_all_tags

  dynamic "launch_template" {
    for_each = var.node_ami_image_id == null ? [] : [aws_launch_template.node_custom_ami[0]]

    content {
      id      = launch_template.value.id
      version = tostring(launch_template.value.latest_version)
    }
  }

  scaling_config {
    desired_size = var.node_desired_size
    min_size     = var.node_min_size
    max_size     = var.node_max_size
  }

  update_config {
    max_unavailable = 1
  }

  labels = var.node_labels

  dynamic "taint" {
    for_each = var.node_taints

    content {
      key    = taint.value.key
      value  = taint.value.value
      effect = taint.value.effect
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.node_worker,
    aws_iam_role_policy_attachment.node_cni,
    aws_iam_role_policy_attachment.node_ecr_readonly,
    aws_iam_role_policy_attachment.node_ssm,
    aws_eks_cluster.this,
    aws_eks_addon.managed,
  ]
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

data "tls_certificate" "oidc" {
  count = var.enable_irsa ? 1 : 0
  url   = aws_eks_cluster.this.identity[0].oidc[0].issuer
}

resource "aws_iam_openid_connect_provider" "this" {
  count = var.enable_irsa ? 1 : 0

  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [data.tls_certificate.oidc[0].certificates[0].sha1_fingerprint]
  url             = aws_eks_cluster.this.identity[0].oidc[0].issuer
  tags            = local.tags
}