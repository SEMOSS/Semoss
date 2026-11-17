# Deploying the IL4 image on Amazon EKS: step by step

> **Status: designed, not yet exercised.** The container, its volumes, secrets,
> CAC requirement and rotation behavior were proved on ECS Fargate
> ([DEPLOY-ECS.md](./DEPLOY-ECS.md)). This runbook carries the same design to
> Kubernetes. The manifests and commands have not been run from this repository
> against a cluster. Where a step depends on behavior I could not confirm, it says
> **Verify** and gives the check. Run the whole procedure in a test cluster and
> complete step 14 before relying on it.

Placeholders: `<account>`, `<region>` (for example `us-gov-west-1`), `<repo>` (ECR
repository), `<cluster>`, `<namespace>` (this runbook uses `semoss`), `<efs-id>`,
`<prefix>`, `<digest>`. In GovCloud, ARNs start `arn:aws-us-gov:`. Keep real
account IDs, ARNs and host names in your change record, not in this repository.

Related: [DEPLOY-ECS.md](./DEPLOY-ECS.md), [README.md](./README.md) (image
contents, persistence, CAC/PIV), [DEPLOYMENT-CHECKLIST.md](./DEPLOYMENT-CHECKLIST.md),
[TLS.md](./TLS.md).

## How the deployment fits together

```
client (CAC) ──TCP 8443, TLS passes through──▶ Network Load Balancer (Service type LoadBalancer)
                                                    │
                                              Pod (StatefulSet)
        initContainer tls-init        ─▶ writes /run/secrets/*   (root, only CAP_CHOWN)
        initContainer seed-semosshome ─▶ fills the home volume once
        container semoss (10001:10001) ─▶ Tomcat on 8443
                    ├─ /opt/semosshome                           ─ PVC (EFS, RWX)
                    ├─ /opt/tomcat/webapps/Monolith/public_home   ─ PVC (EFS, RWX)
                    ├─ /run/secrets                               ─ in-memory emptyDir, read-only in semoss
                    └─ /tmp, tomcat temp/work/logs                ─ emptyDir (read-only root)
                  PostgreSQL holds users, apps, permissions (not on the volumes)
```

The same four rules as ECS: pass TLS through to Tomcat (CAC needs the card's
certificate to arrive), keep both volumes persistent, require the CAC trust store,
and keep PostgreSQL's records and the two volumes backed up together.

---

## Step 1. Decide which image to deploy

Use the same pipelines as ECS, described in step 1 of
[DEPLOY-ECS.md](./DEPLOY-ECS.md): GitHub Actions `il4-container.yml` (copy to ECR by
hand) or the CodeBuild "5.4" project (carries the CAC filter). The CodeBuild "6.0"
project builds Monolith `v6.0.0`, which **has no CAC filter**; do not use it for an
environment that must require a card.

Every build re-resolves the Iron Bank base digests (`refresh_bases.py`), so a fresh
build has the newest OS packages. **Deploy the image by digest**, never by a moving
tag, and write down the harness commit, Monolith commit and base digests from the
build log.

**Check:** you have an `<account>.dkr.ecr.<region>.amazonaws.com/<repo>@sha256:<digest>`
reference.

## Step 2. Publish the image and scan it

Follow step 2 of [DEPLOY-ECS.md](./DEPLOY-ECS.md) (copy to ECR, find the digest, read
the Inspector findings until the count is stable).

**Check:** only the accepted vendor-bundled Jackson and lz4 findings remain. Any OS
finding means the bases are stale: rebuild.

## Step 3. Create or choose the cluster

Requirements: Kubernetes version supported by your EKS release, nodes in private
subnets with the **x86_64** architecture (the image is `linux/amd64`), the cluster's
OIDC provider enabled for IAM roles for service accounts (IRSA), and a node security
group that can reach EFS (TCP 2049), PostgreSQL, and the AWS service endpoints.

```bash
eksctl create cluster --name <cluster> --region <region> --nodes 2 \
  --node-type m6i.xlarge --node-private-networking --with-oidc
```

Use your site's approved cluster definition instead if you have one. Fargate
profiles are not suitable: EFS dynamic provisioning and the controller below need
EC2 nodes in the setup described here.

**Check:** `kubectl get nodes` shows the nodes `Ready` and
`aws eks describe-cluster --name <cluster> --query cluster.identity.oidc.issuer`
returns a URL.

## Step 4. Install the three cluster add-ons

1. **EFS CSI driver** (volumes):

   ```bash
   eksctl create iamserviceaccount --cluster <cluster> --namespace kube-system \
     --name efs-csi-controller-sa --role-only --role-name <prefix>-efs-csi \
     --attach-policy-arn arn:aws-us-gov:iam::aws:policy/service-role/AmazonEFSCSIDriverPolicy \
     --approve
   aws eks create-addon --cluster-name <cluster> --addon-name aws-efs-csi-driver \
     --service-account-role-arn arn:aws-us-gov:iam::<account>:role/<prefix>-efs-csi
   ```

2. **AWS Load Balancer Controller** (the Network Load Balancer): install it with the
   Helm chart and the IAM policy from the AWS documentation, with its service
   account bound by IRSA.
3. **Secrets**: this runbook uses an init container with IRSA (step 7), which needs
   no extra add-on. If your site standardizes on External Secrets or the Secrets
   Store CSI driver, see the note in step 7.

**Check:** `kubectl get pods -n kube-system` shows the EFS CSI and load balancer
controller pods `Running`.

## Step 5. Create the namespace and the service account

```bash
kubectl create namespace semoss
kubectl label namespace semoss pod-security.kubernetes.io/enforce=baseline   # see the note below

# IAM role the pod assumes (secrets, and Bedrock if used)
eksctl create iamserviceaccount --cluster <cluster> --namespace semoss --name semoss \
  --role-name <prefix>-semoss --attach-policy-arn <policy-arn> --approve
```

The policy attached to that role needs:

- `secretsmanager:GetSecretValue` on exactly the four TLS and CAC secrets from step 6
- `bedrock:InvokeModel` and `bedrock:InvokeModelWithResponseStream` on the models you
  enable, if you use Bedrock
- `kms:Decrypt` on the key, if the secrets use a customer-managed key

Pod Security: the `tls-init` init container runs as root (it must `chown` the secret
files to 10001), so the namespace cannot enforce `restricted`. `baseline` allows a
root init container. If your policy forbids root entirely, use the Secrets Store CSI
alternative in step 7 and enforce `restricted`.

**Check:** `kubectl -n semoss get sa semoss -o yaml` shows the
`eks.amazonaws.com/role-arn` annotation.

## Step 6. Create the secrets in Secrets Manager

The same four secrets as ECS step 3. The files are yours; never paste their
contents into a chat, a ticket or a manifest.

```bash
aws secretsmanager create-secret --name <prefix>-tls-keystore --secret-binary fileb://server.bcfks
aws secretsmanager create-secret --name <prefix>-tls-keystore-password --secret-string file://server.password
aws secretsmanager create-secret --name <prefix>-cac-truststore --secret-binary fileb://cac-truststore.bcfks
aws secretsmanager create-secret --name <prefix>-cac-truststore-password --secret-string file://cac-truststore.password
```

Build the CAC trust store from the DoD PKI CA bundle as in
[README.md](./README.md#giving-the-connector-its-trust-store). Its password may
contain only `A-Za-z0-9._@%+=:,/-`. Put the PostgreSQL credentials in Secrets Manager
too, and deliver them to the pod with your site's mechanism (step 9 uses a Kubernetes
Secret created from them; do not commit that Secret to Git).

**Check:** `aws secretsmanager list-secrets --query 'SecretList[].Name'` shows them.

## Step 7. Create the storage

Set up the EFS file system exactly as ECS step 4 (encrypted, a mount target in each
node subnet, a security group allowing TCP 2049 from the node security group,
automatic backups on). You do **not** create access points by hand here; the CSI
driver creates one per volume claim when the storage class says so.

```yaml
# storageclass.yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata: {name: semoss-efs}
provisioner: efs.csi.aws.com
parameters:
  provisioningMode: efs-ap
  fileSystemId: <efs-id>
  directoryPerms: "750"
  uid: "10001"
  gid: "10001"
  basePath: /semoss
reclaimPolicy: Retain          # a deleted claim must not delete the data
volumeBindingMode: Immediate
```

```yaml
# pvcs.yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: semoss-home, namespace: semoss}
spec:
  accessModes: [ReadWriteMany]
  storageClassName: semoss-efs
  resources: {requests: {storage: 50Gi}}     # EFS ignores the size; set a realistic number
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: semoss-public-home, namespace: semoss}
spec:
  accessModes: [ReadWriteMany]
  storageClassName: semoss-efs
  resources: {requests: {storage: 10Gi}}
```

```bash
kubectl apply -f storageclass.yaml -f pvcs.yaml
```

The access point forces `10001:10001`, which is what makes each volume writable by
the application user; the image's root is read-only and cannot do it for you.

**Check:** `kubectl -n semoss get pvc` shows both `Bound`.

## Step 8. Create the configuration

```bash
kubectl -n semoss create configmap semoss-config \
  --from-literal=SEMOSS_REQUIRE_POSTGRES=true \
  --from-literal=CAC_PIV_CLIENT_AUTH=true \
  --from-literal=FE_ROUTE=/Monolith \
  --from-literal=AWS_REGION=<region> \
  --from-literal=AWS_USE_FIPS_ENDPOINT=true
```

Add the system-database connection settings your site uses (the rehearsal task
carries the `CUSTOM_*` database settings). Put credentials in a Kubernetes Secret
(`kubectl create secret generic semoss-db --from-literal=...` from a file or your
secret manager, with encryption at rest enabled on the cluster), and reference it
with `secretKeyRef` below. **Never set `CAC_PIV_REQUIRED=false`** in a deployment: it
is a local-development opt-out, and leaving it unset keeps CAC required.

## Step 9. Deploy the application

Save as `semoss.yaml`, set the image digest in **both** places, and apply it. The
init container seeds the home volume once with the same lock-and-marker script as
ECS, so two pods starting together do not copy twice.

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata: {name: semoss, namespace: semoss}
spec:
  serviceName: semoss
  replicas: 1
  selector: {matchLabels: {app: semoss}}
  template:
    metadata: {labels: {app: semoss}}
    spec:
      serviceAccountName: semoss
      terminationGracePeriodSeconds: 60
      securityContext:
        seccompProfile: {type: RuntimeDefault}
        fsGroup: 10001
      initContainers:
        - name: tls-init
          image: public.ecr.aws/aws-cli/aws-cli:2.27.0    # mirror it into ECR for an offline cluster
          command: ["/bin/sh", "-c"]
          args:
            - |
              set -e
              aws secretsmanager get-secret-value --secret-id "$TLS_KEYSTORE" --query SecretBinary --output text | base64 -d > /run/secrets/server.bcfks
              aws secretsmanager get-secret-value --secret-id "$TLS_PASSWORD" --query SecretString --output text > /run/secrets/server.password
              aws secretsmanager get-secret-value --secret-id "$CAC_TRUSTSTORE" --query SecretBinary --output text | base64 -d > /run/secrets/cac-truststore.bcfks
              aws secretsmanager get-secret-value --secret-id "$CAC_TRUSTSTORE_PASSWORD" --query SecretString --output text > /run/secrets/cac-truststore.password
              chown 10001:10001 /run/secrets/*
              chmod 600 /run/secrets/*
          env:
            - {name: AWS_REGION, value: <region>}
            - {name: TLS_KEYSTORE, value: <prefix>-tls-keystore}
            - {name: TLS_PASSWORD, value: <prefix>-tls-keystore-password}
            - {name: CAC_TRUSTSTORE, value: <prefix>-cac-truststore}
            - {name: CAC_TRUSTSTORE_PASSWORD, value: <prefix>-cac-truststore-password}
          securityContext:
            runAsUser: 0
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: false      # aws-cli writes to its home
            capabilities: {drop: [ALL], add: [CHOWN]}
          volumeMounts:
            - {name: tls, mountPath: /run/secrets}
        - name: seed-semosshome
          image: <account>.dkr.ecr.<region>.amazonaws.com/<repo>@sha256:<digest>
          command: ["/bin/sh", "-c"]
          args:
            - |
              set -eu
              D=/mnt/semosshome
              if [ -f "$D/.seeded" ]; then echo "semosshome already seeded - leaving it alone"; exit 0; fi
              if mkdir "$D/.seed-lock" 2>/dev/null; then
                echo "semosshome volume is empty - seeding from the image payload"
                cp -a /opt/semosshome/. "$D/"
                touch "$D/.seeded"
                rmdir "$D/.seed-lock"
                echo "seeded"
              else
                echo "another pod is seeding semosshome - waiting for it"
                i=0
                while [ ! -f "$D/.seeded" ]; do
                  i=$((i + 1))
                  if [ "$i" -gt 150 ]; then echo "timed out; if no pod is seeding, remove $D/.seed-lock" >&2; exit 1; fi
                  sleep 2
                done
                echo "seeded by the other pod"
              fi
          securityContext:
            runAsUser: 10001
            runAsGroup: 10001
            runAsNonRoot: true
            allowPrivilegeEscalation: false
            capabilities: {drop: [ALL]}
          volumeMounts:
            - {name: semosshome, mountPath: /mnt/semosshome}
      containers:
        - name: semoss
          image: <account>.dkr.ecr.<region>.amazonaws.com/<repo>@sha256:<digest>
          ports: [{name: https, containerPort: 8443}]
          envFrom:
            - configMapRef: {name: semoss-config}
          # add the database settings and secretKeyRef entries for your site here
          securityContext:
            runAsUser: 10001
            runAsGroup: 10001
            runAsNonRoot: true
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities: {drop: [ALL]}
          resources:
            requests: {cpu: "2", memory: 4Gi}
            limits: {memory: 4Gi}
          volumeMounts:
            - {name: tls, mountPath: /run/secrets, readOnly: true}
            - {name: semosshome, mountPath: /opt/semosshome}
            - {name: public-home, mountPath: /opt/tomcat/webapps/Monolith/public_home}
            - {name: tmp, mountPath: /tmp}
            - {name: tomcat-temp, mountPath: /opt/tomcat/temp}
            - {name: tomcat-work, mountPath: /opt/tomcat/work}
            - {name: tomcat-logs, mountPath: /opt/tomcat/logs}
          startupProbe:
            httpGet: {path: /Monolith/health/ready, port: 8443, scheme: HTTPS}
            periodSeconds: 10
            failureThreshold: 30           # up to 5 minutes for the first start
          readinessProbe:
            httpGet: {path: /Monolith/health/ready, port: 8443, scheme: HTTPS}
            periodSeconds: 15
          livenessProbe:
            tcpSocket: {port: 8443}
            periodSeconds: 30
            failureThreshold: 6
      volumes:
        - {name: tls, emptyDir: {medium: Memory}}
        - {name: semosshome, persistentVolumeClaim: {claimName: semoss-home}}
        - {name: public-home, persistentVolumeClaim: {claimName: semoss-public-home}}
        - {name: tmp, emptyDir: {}}
        - {name: tomcat-temp, emptyDir: {}}
        - {name: tomcat-work, emptyDir: {}}
        - {name: tomcat-logs, emptyDir: {}}
```

Notes on this manifest:

- **Verify the read-only root.** The ECS rehearsal ran with a writable root, and the
  hardened compose profile uses a read-only root with writable `/tmp` and the three
  Tomcat directories above. Start a test pod and confirm Tomcat, the Python worker
  and the native libraries start. If something needs another writable path, add an
  `emptyDir` for it rather than dropping `readOnlyRootFilesystem`.
- **Probes.** An `httpGet` HTTPS probe checks the status code only and does not verify
  the certificate or the `startupComplete` field. For the exact check, use
  [readiness.py](./readiness.py), which is not baked into the image (it is bind-mounted
  in the compose profile): put it in a ConfigMap, mount it with the CA that signed the
  server certificate, and use
  `exec: {command: [/opt/semoss-python/bin/python, /probe/readiness.py, --url, https://<name-on-the-certificate>:8443/Monolith/health/ready, --ca, /probe/ca.pem]}`.
  The name must match the certificate and resolve to this pod (for example a
  `hostAliases` entry), and the probe interpreter path **must be verified** in the
  image. The health path is exempt from the CAC requirement, so a probe needs no card.
- `tls-init` secrets live in a memory-backed `emptyDir`, so they never touch node
  disk. **Alternative:** the Secrets Store CSI driver with the AWS provider can mount
  the four secrets as files without a root init container. **Verify** it writes
  binary secrets unchanged and that the files are readable by UID 10001 (mode and
  `fsGroup`) before using it; the entrypoint refuses a trust store it cannot read.
- A single replica is the tested shape. More than one replica needs the same shared
  volumes (they are `ReadWriteMany`), the same PostgreSQL, and session stickiness
  (step 10). See the multi-container notes in
  [README.md](./README.md#persistence-container-rotation-and-more-than-one-container).

```bash
kubectl apply -f semoss.yaml
kubectl -n semoss rollout status statefulset/semoss --timeout=10m
```

**Check:** the pod is `Running` and `Ready`. `kubectl -n semoss logs semoss-0 -c
seed-semosshome` says the volume was empty and seeded (or already seeded on later
starts). `kubectl -n semoss logs semoss-0 -c semoss` reaches Tomcat's `Server startup`
line with no `ERROR`/`SEVERE` in the first minutes.

## Step 10. Expose it with a Network Load Balancer

TLS must pass through to Tomcat, so use a Service of type `LoadBalancer` for an NLB
with a TCP listener, **not** an Ingress or an Application Load Balancer.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: semoss
  namespace: semoss
  annotations:
    service.beta.kubernetes.io/aws-load-balancer-type: external
    service.beta.kubernetes.io/aws-load-balancer-nlb-target-type: ip
    service.beta.kubernetes.io/aws-load-balancer-scheme: internal
    service.beta.kubernetes.io/aws-load-balancer-target-group-attributes: stickiness.enabled=true,stickiness.type=source_ip
spec:
  type: LoadBalancer
  selector: {app: semoss}
  ports:
    - {name: https, port: 8443, targetPort: 8443, protocol: TCP}
```

Source-IP stickiness matters once there is more than one pod, because a login lives
in the memory of the pod that created it. Add a `NetworkPolicy` that allows ingress to
8443 only from the load balancer subnets or the approved client ranges, and egress
only to DNS, EFS (2049), PostgreSQL, the Secrets Manager / KMS / Bedrock endpoints,
and the DoD OCSP responders if you enable revocation checking.

```bash
kubectl apply -f service.yaml
kubectl -n semoss get svc semoss -w          # wait for the EXTERNAL-IP / hostname
```

**Check:** the NLB target group shows the pod IP `healthy`.

## Step 11. Verify the first start

From a host with a route to the NLB, and the CA that signed the server certificate:

```bash
curl --fail --cacert approved-ca.pem https://<dns-name>:8443/Monolith/health/ready
```

It must return HTTP 200 with `startupComplete: true`. HTTP 200 from the UI alone is
not enough.

## Step 12. Verify CAC is enforced and works

1. **Without a card:** `curl -sk https://<dns-name>:8443/Monolith/api/config` returns
   **403** with `{"errorType":"CAC_REQUIRED",...}`. A browser with no card that opens
   a page under `/Monolith/` is redirected to `/Monolith/cacRequired/`. The pod log
   shows one audit line per minute per client:
   `a CAC/PIV client certificate is required and none was presented`.
2. **With a card:** insert the card, **quit the browser completely**, reopen, browse to
   `https://<dns-name>:8443/SemossWeb/packages/client/dist/`, choose the certificate
   and sign in. Chrome caches "no certificate" for a TLS session, so a reload after
   inserting the card does not prompt again.
3. A user without a card who opens `/SemossWeb/...` sees the front end's generic
   "Something went wrong!" page: that web application is separate, its static files
   load without a certificate, and its API calls to `/Monolith/` return the 403. It is
   the requirement working; the audit line above confirms it.
4. **Source address.** Behind an NLB with `ip` targets the client address may be the
   NLB's. **Verify** that the audit line and access log show what your policy needs;
   the CAC filter's loopback exemption looks only at the TCP peer address, never at
   `X-Forwarded-For`.

## Step 13. Prove persistence with a pod rotation

1. **Before.** Sign in and open (or import) an app. Record the app's `.smss` size
   (`/opt/semosshome/project/<name>__<id>.smss`), its rows in the `project` and
   `projectpermission` tables, and that its portal returns 200 with a byte count
   (`GET /Monolith/public_home/<id>/portals/ 200 <bytes>` in the access log).
2. **Replace every pod.**

   ```bash
   kubectl -n semoss rollout restart statefulset/semoss
   kubectl -n semoss rollout status statefulset/semoss --timeout=10m
   ```

   With one replica this is an outage until the new pod is ready.
3. **After.** The same app is listed, with the same permissions and the same portal
   byte count, and `kubectl -n semoss logs semoss-0 -c seed-semosshome` says
   `already seeded`.

If an app is missing or its portal is empty, **stop** and look at the volumes (step 16)
before re-importing: importing an app that still has records can delete its
permissions.

## Step 14. Acceptance before you rely on it

Because this runbook has not been run, do all of the following in a test cluster and
keep the output:

- Pod starts, seeds, and reaches ready (step 9)
- Read-only root works, or the extra writable paths are identified (step 9 note)
- NLB passes the card's certificate through (steps 11 and 12)
- A rotation keeps apps, permissions and portals (step 13)
- Two replicas (if you will run them) share both volumes and the database, and
  stickiness keeps a login on one pod
- Inspector count for the digest matches the accepted set

## Step 15. Deploy a new image later

1. Build, publish and scan (steps 1 and 2).
2. Take a PostgreSQL snapshot and confirm the EFS backup is current.
3. Change **only** the image digest, in both `seed-semosshome` and `semoss`:
   `kubectl -n semoss set image statefulset/semoss seed-semosshome=<image> semoss=<image>`
   (init containers are changed with `kubectl patch` or by editing the manifest).
4. `kubectl -n semoss rollout status statefulset/semoss`, then repeat steps 11 to 13.
5. **Rollback:** `kubectl -n semoss rollout undo statefulset/semoss`. Volumes and
   databases are unchanged by a rollback; restore the PostgreSQL snapshot first if
   the new image changed the schema.

The seed step never overwrites a populated home volume, so an image upgrade does not
touch users' apps, and **settings added in a newer image do not appear in an existing
home volume**: see the migration notes in [README.md](./README.md). Moving an older
deployment to this image also needs the `public_home` claim (step 7) and the CAC trust
store secrets (step 6), or published apps are lost on restart and the container will
not start.

## Step 16. Troubleshooting

| Symptom | Cause and action |
| --- | --- |
| Pod `Init:Error` at `tls-init` | The service account's IAM role lacks `secretsmanager:GetSecretValue` on a secret (or the KMS key), or IRSA is not bound; `kubectl logs semoss-0 -c tls-init`. |
| Pod crash-loops with a CAC message | Trust store or its password file missing, unreadable by 10001, or the password has a disallowed character. Do not set `CAC_PIV_REQUIRED=false` in a deployment. |
| PVC stays `Pending` | EFS CSI driver not installed, mount target missing in a node subnet, security group blocks 2049, or the storage class `fileSystemId` is wrong. |
| Pod `Running` but never `Ready` | Look for `startupComplete` in the `semoss` log; raise the startup probe `failureThreshold`. |
| Target group unhealthy | Node or load balancer security group blocks 8443, or a `NetworkPolicy` blocks the health check. |
| Users asked to log in repeatedly | More than one pod and no source-IP stickiness on the target group. |
| Published apps vanish after a restart | `public_home` is on an `emptyDir` or other ephemeral storage. |
| App page shows 404 and `Could not find the requested resource` | The portal folder is empty. Opening an app can re-copy its portal from the project's source `portals` folder into `public_home`; if the source is empty the copy replaces a working portal with nothing. Confirm by listing both folders from a read-only debug pod (below). Recover by re-importing from the export, after reading "Never import an app that already exists" in [README.md](./README.md#persistence-container-rotation-and-more-than-one-container). |
| After inserting a card the browser still shows the error page | Quit the browser fully and reopen. |
| The front end shows "Something went wrong!" | Usually no certificate was presented; see step 12, item 3. |

**Diagnosing a volume without changing it.** Run a short pod that mounts both claims
with `readOnly: true`, prints `id`, `ls -lan` of the `public_home` folder,
`find <public_home>/<id> -ls` and the project's
`app_root/version/assets` folder, then delete it:

```bash
kubectl -n semoss run vol-check --rm -it --restart=Never --image=<image> --overrides='<json with the two read-only claims>' -- /bin/sh
```

## Step 17. Evidence to keep with the change

Image digest; harness commit, Monolith commit and the three base digests from the
build log; the Inspector count for that digest; the manifest (or its hash) applied;
results of steps 11 to 14; and the date of the last PostgreSQL snapshot and EFS
backup.
