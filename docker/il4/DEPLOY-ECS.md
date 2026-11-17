# Deploying the IL4 image on Amazon ECS (Fargate): step by step

Every step below was performed in a GovCloud rehearsal and test environment
(`us-gov-west-1`, AWS CLI partition `aws-us-gov`). Follow them in order; each
ends with a check, so you know the step worked before the next one depends on it.

Placeholders: `<account>`, `<region>` (for example `us-gov-west-1`), `<repo>` (ECR
repository), `<cluster>`, `<service>`, `<vpc>`, `<subnet-a>`, `<subnet-b>`,
`<task-sg>`, `<efs-id>`, `<prefix>` (a name prefix for everything you create).
Keep real account IDs, ARNs and host names in your change record, not in this
repository. In GovCloud, ARNs start `arn:aws-us-gov:`.

Related: [DEPLOY-EKS.md](./DEPLOY-EKS.md) (the Kubernetes version),
[README.md](./README.md) (what is in the image, persistence, CAC/PIV),
[DEPLOYMENT-CHECKLIST.md](./DEPLOYMENT-CHECKLIST.md) (acceptance evidence),
[TLS.md](./TLS.md) (the server certificate).

## How the deployment fits together

```
client (CAC) ──TCP 8443, TLS passes through──▶ Network Load Balancer
                                                    │
                                          ECS service (Fargate task)
        tls-init ─▶ writes /run/secrets/*   (non-essential, runs first)
        seed-semosshome ─▶ fills the EFS home volume once   (non-essential)
        semoss ─▶ Tomcat on 8443, runs as 10001:10001       (essential)
                    ├─ /opt/semosshome                                  ─ EFS access point 1
                    ├─ /opt/tomcat/webapps/Monolith/public_home          ─ EFS access point 2
                    └─ /run/secrets   (TLS + CAC trust store, read-only)
                  PostgreSQL holds users, apps, permissions (not on EFS)
```

Why these choices matter:

- **TLS passes through the load balancer.** A CAC login needs the card's
  certificate to reach Tomcat, so terminate nothing at the load balancer.
- **An app is two halves.** Its records live in PostgreSQL; its files live on
  `/opt/semosshome`; its published portal lives on `public_home`. Lose any one and
  the app disappears. Both volumes must be persistent.
- **CAC is required by default.** The container will not start without a DoD CA
  trust store (step 3).

---

## Step 1. Decide which image to deploy

| Build | Result | Notes |
| --- | --- | --- |
| GitHub Actions `il4-container.yml` on `IL4-dev` | `ghcr.io/semoss/semoss-il4:sha-<commit>-<run>-1` | The reference build, with its tests. You copy it to ECR (step 2). |
| CodeBuild "5.4" project (source `IL4-dev`) | `<repo>-semoss-il4:<sha12>-ov<overlay>` | Includes the CAC filter. This is what the rehearsal environment ran. |
| CodeBuild "6.0" project | `<repo>-semoss-6-0:v6.0.0-<timestamp>` | SEMOSS 6.0. **No CAC filter** (it is not in Monolith `v6.0.0`) and extra Node scan findings. Use only to test 6.0. |

To build with CodeBuild:

```bash
aws codebuild start-build --project-name <project> --region <region>
aws codebuild batch-get-builds --ids <build-id> --query 'builds[0].[buildStatus,currentPhase]'
```

It takes about 15 minutes. Every build re-resolves the Iron Bank base digests
(`refresh_bases.py`), so a fresh build picks up the newest OS packages. To move to a
new UBI minor release, change `UBI_IMAGE` in the `Dockerfile`, run
`python3 refresh_bases.py`, commit, and rebuild; check that the Python donor image's
`mil.dso.ironbank.image.parent` label names the same UBI minor.

**Check:** the build log lines `IL4-dev harness commit`, `Monolith commit` and the
three resolved base digests are present. Write them down.

## Step 2. Get the image into ECR and scan it

If you used GitHub Actions, copy the image (Docker running; `--platform` matters on
Apple silicon):

```bash
SRC=ghcr.io/semoss/semoss-il4:sha-<fullsha>-<runid>-1
DST=<account>.dkr.ecr.<region>.amazonaws.com/<repo>:il4-dev-<sha12>
docker pull --platform linux/amd64 "$SRC"
docker tag "$SRC" "$DST"
aws ecr get-login-password --region <region> | docker login --username AWS \
  --password-stdin <account>.dkr.ecr.<region>.amazonaws.com
docker push "$DST"
```

CodeBuild pushes to ECR on its own. Find the digest, then read the Inspector
findings for it, polling until the number stops changing (it climbs while the first
scan runs):

```bash
aws ecr describe-images --repository-name <repo> --image-ids imageTag=<tag> \
  --query 'imageDetails[0].imageDigest' --output text
aws inspector2 list-findings --region <region> \
  --filter '{"ecrImageHash":[{"comparison":"EQUALS","value":"sha256:<digest>"}]}' \
  --query 'length(findings)'
```

**Check:** only the accepted vendor-bundled Jackson and lz4 findings remain (12 on
the reference image). Any OS-package finding means the bases are stale: rebuild.

## Step 3. Create the secrets

Create five kinds of secret in Secrets Manager (add `--kms-key-id` for a
customer-managed key). The files are yours; never paste their contents into a chat,
ticket or the task definition.

```bash
# server identity: BCFKS keystore with the private key and full chain, and its password
aws secretsmanager create-secret --name <prefix>-tls-keystore \
  --secret-binary fileb://server.bcfks
aws secretsmanager create-secret --name <prefix>-tls-keystore-password \
  --secret-string file://server.password

# CAC trust store: BCFKS of the DoD PKI root and intermediate CAs, and its password
aws secretsmanager create-secret --name <prefix>-cac-truststore \
  --secret-binary fileb://cac-truststore.bcfks
aws secretsmanager create-secret --name <prefix>-cac-truststore-password \
  --secret-string file://cac-truststore.password
```

- Build the CAC trust store as described in
  [README.md](./README.md#giving-the-connector-its-trust-store). Its password may
  contain only `A-Za-z0-9._@%+=:,/-`; `openssl rand -hex 24` is fine.
- The server certificate's names must match the DNS name clients use.
- Create the PostgreSQL credential(s) the same way (a JSON secret with `username`
  and `password` keys works with ECS `secrets` references such as
  `<secret-arn>:username::`).

**Check:** `aws secretsmanager list-secrets --query 'SecretList[].Name'` shows all of
them. Do not run `get-secret-value`.

## Step 4. Create the shared storage (EFS)

```bash
aws efs create-file-system --encrypted --performance-mode generalPurpose \
  --throughput-mode elastic --tags Key=Name,Value=<prefix>-efs --region <region>
# one mount target per task subnet, with a security group that allows TCP 2049 from <task-sg>
aws efs create-mount-target --file-system-id <efs-id> --subnet-id <subnet-a> --security-groups <efs-sg>
aws efs create-mount-target --file-system-id <efs-id> --subnet-id <subnet-b> --security-groups <efs-sg>

# two access points, both forcing 10001:10001
aws efs create-access-point --file-system-id <efs-id> \
  --posix-user Uid=10001,Gid=10001 \
  --root-directory 'Path=/semosshome,CreationInfo={OwnerUid=10001,OwnerGid=10001,Permissions=750}' \
  --tags Key=Name,Value=<prefix>-semosshome
aws efs create-access-point --file-system-id <efs-id> \
  --posix-user Uid=10001,Gid=10001 \
  --root-directory 'Path=/public_home,CreationInfo={OwnerUid=10001,OwnerGid=10001,Permissions=750}' \
  --tags Key=Name,Value=<prefix>-public-home
```

The access point is what makes the volume writable by UID 10001; the image's
read-only root cannot do it for you. Keep the two access point IDs (`fsap-...`).

**Check:** `aws efs describe-mount-targets --file-system-id <efs-id>` shows
`available` for each subnet, and `describe-access-points` shows both access points.
Enable automatic backups (`aws efs put-backup-policy --backup-policy Status=ENABLED`).
Back the files up **with** the PostgreSQL databases; a volume backup alone loses
permissions and a database backup alone loses the app's code.

## Step 5. Create the IAM roles

**Execution role** (ECS uses it to start the task): the AWS-managed
`AmazonECSTaskExecutionRolePolicy`, plus `secretsmanager:GetSecretValue` on the
database secrets referenced by the container's `secrets`, plus `kms:Decrypt` if you
use a customer-managed key.

**Task role** (the running containers use it, including `tls-init`):

- `secretsmanager:GetSecretValue` on exactly the four TLS and CAC secrets
- `elasticfilesystem:ClientMount` and `elasticfilesystem:ClientWrite` on the file
  system (the volumes use IAM authorization)
- `bedrock:InvokeModel` and `bedrock:InvokeModelWithResponseStream` on the models you
  enable, if you use Bedrock
- `kms:Decrypt` for the key, if any

Trust policy for both: `ecs-tasks.amazonaws.com`.

**Check:** `aws iam simulate-principal-policy` for the task role against the secret
ARNs and the file system returns `allowed`.

## Step 6. Network and load balancer

```bash
# security groups: task-sg allows TCP 8443 from clients / the NLB and outbound to EFS (2049),
# PostgreSQL, the Secrets Manager / KMS / Bedrock endpoints, and ECR
aws elbv2 create-load-balancer --name <prefix>-nlb --type network --scheme internal \
  --subnets <subnet-a> <subnet-b>
aws elbv2 create-target-group --name <prefix>-tg --protocol TCP --port 8443 \
  --target-type ip --vpc-id <vpc> \
  --health-check-protocol HTTPS --health-check-path /Monolith/health/ready \
  --health-check-port 8443
aws elbv2 modify-target-group-attributes --target-group-arn <tg-arn> \
  --attributes Key=stickiness.enabled,Value=true Key=stickiness.type,Value=source_ip
aws elbv2 create-listener --load-balancer-arn <nlb-arn> --protocol TCP --port 8443 \
  --default-actions Type=forward,TargetGroupArn=<tg-arn>
```

Do **not** create a TLS listener or an application load balancer: either would
terminate TLS and the card's certificate would never reach Tomcat. Stickiness
matters once you run more than one task, because a login lives in the memory of the
task that created it. If the VPC has no route to the AWS APIs, add interface
endpoints for ECR (api and dkr), Secrets Manager, KMS, CloudWatch Logs, Bedrock and
the S3 gateway endpoint ECR needs.

**Check:** the listener exists on 8443 and the target group type is `TCP`.

## Step 7. Create the log group

```bash
aws logs create-log-group --log-group-name /ecs/<prefix>-semoss --region <region>
aws logs put-retention-policy --log-group-name /ecs/<prefix>-semoss --retention-in-days <days>
```

Set retention to your audit policy. These are diagnostic logs, not the audit trail
(see "Security audit logging" in [README.md](./README.md)).

## Step 8. Register the task definition

Save as `taskdef.json`, replacing placeholders. Use the image **digest** in both
places that name the app image. Database and other connection settings go in
`environment`, credentials in `secrets` (not shown in full here; the rehearsal
definition carries the system-database `CUSTOM_*` settings and the matching
username/password `secrets`).

```json
{
  "family": "<prefix>-semoss",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "2048",
  "memory": "4096",
  "executionRoleArn": "<execution-role-arn>",
  "taskRoleArn": "<task-role-arn>",
  "volumes": [
    {"name": "tls-secrets"},
    {"name": "semosshome", "efsVolumeConfiguration": {
      "fileSystemId": "<efs-id>", "rootDirectory": "/", "transitEncryption": "ENABLED",
      "authorizationConfig": {"accessPointId": "<semosshome-access-point>", "iam": "ENABLED"}}},
    {"name": "public-home", "efsVolumeConfiguration": {
      "fileSystemId": "<efs-id>", "rootDirectory": "/", "transitEncryption": "ENABLED",
      "authorizationConfig": {"accessPointId": "<public-home-access-point>", "iam": "ENABLED"}}}
  ],
  "containerDefinitions": [
    {
      "name": "tls-init",
      "image": "public.ecr.aws/aws-cli/aws-cli:2.27.0",
      "essential": false,
      "entryPoint": ["/bin/sh", "-c"],
      "command": ["set -e\naws secretsmanager get-secret-value --secret-id \"$TLS_KEYSTORE_SECRET_ARN\" --query SecretBinary --output text | base64 -d > /run/secrets/server.bcfks\naws secretsmanager get-secret-value --secret-id \"$TLS_PASSWORD_SECRET_ARN\" --query SecretString --output text > /run/secrets/server.password\naws secretsmanager get-secret-value --secret-id \"$CAC_TRUSTSTORE_SECRET_ARN\" --query SecretBinary --output text | base64 -d > /run/secrets/cac-truststore.bcfks\naws secretsmanager get-secret-value --secret-id \"$CAC_TRUSTSTORE_PASSWORD_SECRET_ARN\" --query SecretString --output text > /run/secrets/cac-truststore.password\nchown 10001:10001 /run/secrets/server.bcfks /run/secrets/server.password /run/secrets/cac-truststore.bcfks /run/secrets/cac-truststore.password\nchmod 600 /run/secrets/server.bcfks /run/secrets/server.password /run/secrets/cac-truststore.bcfks /run/secrets/cac-truststore.password\n"],
      "environment": [
        {"name": "AWS_REGION", "value": "<region>"},
        {"name": "TLS_KEYSTORE_SECRET_ARN", "value": "<arn>"},
        {"name": "TLS_PASSWORD_SECRET_ARN", "value": "<arn>"},
        {"name": "CAC_TRUSTSTORE_SECRET_ARN", "value": "<arn>"},
        {"name": "CAC_TRUSTSTORE_PASSWORD_SECRET_ARN", "value": "<arn>"}
      ],
      "mountPoints": [{"sourceVolume": "tls-secrets", "containerPath": "/run/secrets"}],
      "logConfiguration": {"logDriver": "awslogs", "options": {"awslogs-group": "/ecs/<prefix>-semoss", "awslogs-region": "<region>", "awslogs-stream-prefix": "tls-init"}}
    },
    {
      "name": "seed-semosshome",
      "image": "<account>.dkr.ecr.<region>.amazonaws.com/<repo>@sha256:<digest>",
      "essential": false,
      "entryPoint": ["/bin/sh", "-c"],
      "command": ["set -eu\nD=/mnt/semosshome\nif [ -f \"$D/.seeded\" ]; then echo \"semosshome already seeded - leaving it alone\"; exit 0; fi\nif mkdir \"$D/.seed-lock\" 2>/dev/null; then\n  echo \"semosshome volume is empty - seeding from the image payload\"\n  cp -a /opt/semosshome/. \"$D/\"\n  touch \"$D/.seeded\"\n  rmdir \"$D/.seed-lock\"\n  echo \"seeded\"\nelse\n  echo \"another task is seeding semosshome - waiting for it\"\n  i=0\n  while [ ! -f \"$D/.seeded\" ]; do\n    i=$((i + 1))\n    if [ \"$i\" -gt 150 ]; then echo \"timed out; if no task is seeding, remove $D/.seed-lock\" >&2; exit 1; fi\n    sleep 2\n  done\n  echo \"seeded by the other task\"\nfi\n"],
      "mountPoints": [{"sourceVolume": "semosshome", "containerPath": "/mnt/semosshome"}],
      "logConfiguration": {"logDriver": "awslogs", "options": {"awslogs-group": "/ecs/<prefix>-semoss", "awslogs-region": "<region>", "awslogs-stream-prefix": "seed"}}
    },
    {
      "name": "semoss",
      "image": "<account>.dkr.ecr.<region>.amazonaws.com/<repo>@sha256:<digest>",
      "essential": true,
      "portMappings": [{"containerPort": 8443, "protocol": "tcp"}],
      "dependsOn": [
        {"containerName": "tls-init", "condition": "COMPLETE"},
        {"containerName": "seed-semosshome", "condition": "SUCCESS"}
      ],
      "environment": [
        {"name": "SEMOSS_REQUIRE_POSTGRES", "value": "true"},
        {"name": "CAC_PIV_CLIENT_AUTH", "value": "true"},
        {"name": "FE_ROUTE", "value": "/Monolith"},
        {"name": "AWS_REGION", "value": "<region>"},
        {"name": "AWS_USE_FIPS_ENDPOINT", "value": "true"}
      ],
      "mountPoints": [
        {"sourceVolume": "tls-secrets", "containerPath": "/run/secrets", "readOnly": true},
        {"sourceVolume": "semosshome", "containerPath": "/opt/semosshome"},
        {"sourceVolume": "public-home", "containerPath": "/opt/tomcat/webapps/Monolith/public_home"}
      ],
      "stopTimeout": 60,
      "logConfiguration": {"logDriver": "awslogs", "options": {"awslogs-group": "/ecs/<prefix>-semoss", "awslogs-region": "<region>", "awslogs-stream-prefix": "semoss"}}
    }
  ]
}
```

Rules for this file:

- **Never set `CAC_PIV_REQUIRED=false`** in a deployment. It is a local-development
  opt-out. Leaving it unset keeps CAC required.
- `SEMOSS_REQUIRE_POSTGRES=true` makes the container refuse to start rather than fall
  back to throwaway embedded storage. Add the system-database settings and `secrets`
  entries your site uses.
- `readonlyRootFilesystem` was not set in the rehearsal tasks. The hardened compose
  profile uses a read-only root with writable `/tmp`, `/opt/tomcat/temp`,
  `/opt/tomcat/work` and `/opt/tomcat/logs`. To adopt it on Fargate, add task-local
  volumes for those four paths, set `readonlyRootFilesystem: true` on `semoss`, and
  check startup in a test service first.

```bash
aws ecs register-task-definition --cli-input-json file://taskdef.json --region <region>
```

**Check:** the command returns a `taskDefinitionArn` with `revision: 1`.

## Step 9. Create the service

```bash
aws ecs create-cluster --cluster-name <cluster> --region <region>   # once
aws ecs create-service --cluster <cluster> --service-name <service> \
  --task-definition <prefix>-semoss:1 --desired-count 1 \
  --launch-type FARGATE --platform-version LATEST \
  --network-configuration 'awsvpcConfiguration={subnets=[<subnet-a>,<subnet-b>],securityGroups=[<task-sg>],assignPublicIp=DISABLED}' \
  --load-balancers 'targetGroupArn=<tg-arn>,containerName=semoss,containerPort=8443' \
  --health-check-grace-period-seconds 180 \
  --deployment-configuration 'minimumHealthyPercent=0,maximumPercent=100'
```

The 180-second grace period covers the first start (seeding the home volume, then
starting the Python and model engines). **`minimumHealthyPercent=0` with
`maximumPercent=100` stops every task before starting replacements, so a deployment
or rotation is an outage of several minutes.** For no downtime run two tasks and use
`minimumHealthyPercent=100`, `maximumPercent=200`; that needs both volumes on shared
storage, the same PostgreSQL, and session stickiness (step 6).

**Check:** wait for the service to reach steady state:

```bash
aws ecs wait services-stable --cluster <cluster> --services <service>
aws ecs describe-services --cluster <cluster> --services <service> \
  --query 'services[0].[runningCount,pendingCount,deployments[0].rolloutState]'
```

## Step 10. Verify the first start

1. In the log group, the `seed` stream says the volume was empty and was seeded (or
   already seeded on later starts); the `semoss` stream reaches Tomcat's
   `Server startup` line and then shows `GET /Monolith/health/ready 200`. No
   `ERROR`/`SEVERE` lines in the first minutes (a `SEVERE` NullPointerException from
   `DBLoader.listenerStop` appears in the **old** task's log at shutdown; it is
   shutdown noise, not a start-up fault).
2. The target group shows the target `healthy`:
   `aws elbv2 describe-target-health --target-group-arn <tg-arn>`.
3. Readiness, from a host with a route to the NLB and the CA that signed the server
   certificate:

   ```bash
   curl --fail --cacert approved-ca.pem https://<dns-name>:8443/Monolith/health/ready
   ```

   It must return HTTP 200 with `startupComplete: true`. HTTP 200 from the UI alone
   is not enough.

## Step 11. Verify CAC is enforced and works

1. **Without a card:**
   `curl -sk https://<dns-name>:8443/Monolith/api/config` must return **403** with
   `{"errorType":"CAC_REQUIRED",...}`. A browser with no card that opens a page under
   `/Monolith/` is redirected to `/Monolith/cacRequired/`, which explains the problem
   and has a "Try again" link. The log shows one audit line per minute per client:
   `a CAC/PIV client certificate is required and none was presented`.
2. **With a card:** insert the card, **quit the browser completely**, reopen, browse
   to `https://<dns-name>:8443/SemossWeb/packages/client/dist/`, choose the
   certificate, and sign in. Chrome caches "no certificate" for a TLS session, so a
   page reload after inserting the card does not prompt again.
3. A user without a card who opens `/SemossWeb/...` directly sees the front end's
   generic "Something went wrong!" page rather than the friendly page: that web
   application is separate and its static files load without a certificate, then its
   API calls to `/Monolith/` return the 403. It is the requirement working. Look for
   the audit line above to confirm.

## Step 12. Prove persistence with a task rotation

Do this once before relying on the environment, and after any storage or image
change.

1. **Before.** Sign in, open (or import) an app. Record: the app's `.smss` file size
   (it lives in `/opt/semosshome/project/<name>__<id>.smss`), the app's rows in the
   `project` and `projectpermission` tables, and that its portal returns 200 with a
   byte count in the Tomcat access log
   (`GET /Monolith/public_home/<id>/portals/ 200 <bytes>`).
2. **Rotate every task.**

   ```bash
   aws ecs update-service --cluster <cluster> --service <service> --force-new-deployment
   aws ecs wait services-stable --cluster <cluster> --services <service>
   ```

   With the settings in step 9 the service is unavailable until the new tasks pass
   their health check.
3. **After.** Reload (quit and reopen the browser if you were signed out and need the
   card again). The same app is listed, with the same permissions and the same portal
   byte count, and the new task's `seed` stream says `already seeded`.

If an app is missing or its portal is empty after the rotation, **stop**. Do not
re-import to "fix forward" until you have looked at the volumes (step 15): re-importing
an app that still has records can delete its permissions.

## Step 13. Deploy a new image later

1. Build, publish and scan the image (steps 1 and 2). Compare findings with the last
   release.
2. Take a PostgreSQL snapshot and make sure the EFS backup is current.
3. Register a new task definition revision that changes **only** the image digest, in
   both `seed-semosshome` and `semoss`.
4. `aws ecs update-service --cluster <cluster> --service <service> --task-definition <prefix>-semoss:<new>`.
5. Wait for steady state, then repeat steps 10 and 11, and step 12's "after" checks.

The seed step never overwrites a populated home volume, so an image upgrade does not
touch users' apps. For the same reason, **settings added in a newer image do not
appear in an existing home volume**: see the migration notes in
[README.md](./README.md). Moving an older deployment to this image also needs the
`public_home` volume (step 4 and the mount in step 8) and the CAC trust store secrets
(step 3), or published apps are lost on restart and the container will not start.

## Step 14. Roll back

Point the service at the previous task definition revision:

```bash
aws ecs update-service --cluster <cluster> --service <service> --task-definition <prefix>-semoss:<previous>
```

The volumes and databases are unchanged by a rollback, so the previous image sees
what it saw before. If the new image changed database schema, restore the PostgreSQL
snapshot taken in step 13 first.

## Step 15. Troubleshooting

| Symptom | Cause and action |
| --- | --- |
| Task stops at start; log says a CAC trust store is required | `cac-truststore.bcfks` / `.password` missing, unreadable, or the password has a disallowed character. Fix the secrets; do not set `CAC_PIV_REQUIRED=false` in a deployment. |
| `tls-init` fails | Task role lacks `secretsmanager:GetSecretValue` on a secret, or the KMS key. |
| Task never mounts EFS | Mount target missing in the task's subnet, security group blocks 2049, or the task role lacks `ClientMount`/`ClientWrite`. |
| Target stays unhealthy | Grace period too short, or the security group blocks the NLB's health check to 8443. Look at the `semoss` log for `startupComplete`. |
| Users asked to log in repeatedly | More than one task and no source-IP stickiness on the NLB target group. |
| Published apps vanish after a restart | `public_home` is not on persistent storage. |
| App page shows 404 and `Could not find the requested resource` | The portal folder is empty. Opening an app can re-copy its portal from the project's source `portals` folder into `public_home`; if the source is empty the copy replaces a working portal with nothing. Confirm by listing `public_home/<id>/portals` and `semosshome/project/<name>__<id>/app_root/version/assets/portals` from a read-only diagnostic task (below). Recover by re-importing the app from its export, after reading "Never import an app that already exists" in [README.md](./README.md#persistence-container-rotation-and-more-than-one-container). |
| After inserting a card the browser still shows the error page | Quit the browser fully and reopen (cached "no certificate" TLS session). |
| The front end shows "Something went wrong!" | Usually no certificate was presented; see step 11, item 3. Check the log for the CAC audit line, and the access log for 403s on `/Monolith/api/`. |
| Extra Node findings | You deployed the 6.0 pipeline's image. Compare like with like. |

**Diagnosing a volume without changing it.** Register a throwaway task definition
that mounts both EFS volumes with `"readOnly": true` and runs a script such as
`id; ls -lan <public_home>; find <public_home>/<id> -ls; find
/opt/semosshome/project/<name>__<id>/app_root/version/assets -maxdepth 2 -ls`, run it
once with `aws ecs run-task`, read its log stream, then
`aws ecs deregister-task-definition` it.

## Step 16. Evidence to keep with the change

Image digest; the harness commit, Monolith commit and three base digests from the
build log; the Inspector count for that digest; the task definition revision;
results of steps 10 to 12; and the date of the last PostgreSQL snapshot and EFS
backup.
