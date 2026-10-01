# Reusable IL4-oriented configuration and deployment checklist

## Purpose and limits

Use this template to prepare and assess a SEMOSS deployment in an approved
environment. It is not an IL4 authorization, a complete NIST control assessment,
or evidence that any arbitrary hosting environment is suitable for CUI.
The applicable DoD requirements, tailored baseline, organization-defined
parameters, inherited controls, and Authorizing Official (AO) decisions govern.

The portable unit is **a tested image plus a site-specific deployment profile**,
not an image containing every site's identity, network, storage, and audit policy.
Some items below require infrastructure or application integration, not a
container configuration switch.

FIPS alternatives are being evaluated; the default Java, providers, cipher
settings, and TLS requirements remain unchanged. See the
[alternative-provider feasibility results](./README.md#alternative-provider-feasibility-2026-10-01).
Neither the Red Hat/NSS investigation nor the isolated ACCP compatibility probe
established an approved replacement. Unresolved module/version,
operating-environment, and Python cryptography evidence remains an open
authorization consideration; a paused migration does not waive requirements.

Keep the completed checklist and evidence in the site's approved evidence
repository. This source repository is public. Do not commit credentials, CUI,
internal topology, sensitive scan findings, or deployment-specific evidence here.

## 1. Portable configuration baseline

Status describes the current template, not every running deployment.
Control references are starting points for mapping, not full control satisfaction.

| Area | Generic setting or pattern | Current state and site-specific limits | Example control mapping |
| --- | --- | --- | --- |
| Image identity | Deploy an approved immutable image digest; record source and base digests. | Build records digests. Select and approve the deployment digest separately; do not automatically promote each refreshed development build. | CM-2, CM-8, SI-7 |
| Process privilege | Non-root, no privileged mode, drop capabilities, prevent privilege escalation; no Docker socket or unnecessary host namespaces/devices. | Image uses UID/GID 10001. Optional Compose profile supplies runtime restrictions. Sites using arbitrary UIDs need a tested adaptation, not a blanket root override. | AC-6, CM-7 |
| Filesystem | Read-only root; explicit writable home; separate noexec/nosuid/nodev temporary storage where supported. | Available in the opt-in profile. Enforce equivalent mounts in the target platform and verify application/native-library behavior. | CM-6, CM-7 |
| Resource bounds | Set CPU, memory, PID, temporary-storage, and persistent-storage limits; define graceful termination. | Profile starts with 4 CPUs, 8 GiB RAM, 512 PIDs, and 60 seconds to stop. These are development defaults, not IL4-prescribed values. Size storage and all limits for the workload. | SC-5, CP-10 |
| Exposure | Expose only the intended application endpoint through the approved ingress. | Tomcat listens on HTTPS 8443 without HTTP/AJP/shutdown listeners. Loopback publishing in the Compose example is local-development behavior, not a production ingress design. | CM-7, SC-7 |
| Secrets | Supply files/secret references at deployment time, never in image layers, build arguments, URLs, or logs. Fail explicitly if required material is absent. | TLS uses required read-only files. Connector secret-backend support must be configured and tested; generic environment substitution is not implemented. | IA-5, SC-12 |
| Sessions | Secure/HttpOnly cookies, deliberate SameSite policy, finite inactivity timeout, self-registration disabled by default. | New homes/webapps use a 15-minute timeout and SameSite=Lax. Validate organizational policy and SSO flows; do not assume every existing volume inherited new defaults. | AC-2, AC-12, SC-23 |
| Readiness | Check the backend, not just the UI; use bounded timeouts and certificate/hostname verification. Separate readiness from liveness and startup handling. | Optional helper requires HTTP 200 and exact `startupComplete: true`. Compose health status alone does not restart an unhealthy container. | SI-4, SC-5 |
| Diagnostic logging | Structured or parseable timestamps, bounded local buffering, no secrets/request bodies/query strings by default. | Tomcat console/access output is configured; opt-in Docker logging rotates three 10 MB files. Neither is centralized audit retention. | AU-3, AU-4, AU-12 |
| Inventory and findings | Generate SBOM/scan reports for the exact final image; record tool/database versions and report failures explicitly. | Optional post-publication reporting is off by default. Findings do not block development publishing. Production promotion requires the site's disposition rules. | CM-8, RA-5, SI-2 |
| Change visibility | Report configuration drift and maintain reviewed deployment manifests; do not silently repair live configuration. | Read-only Docker runtime audit compares a redacted, versioned metadata snapshot with an approved prior report. It covers selected settings, not full manifest/content equivalence; other runtimes need equivalent inspection. | CM-3, CM-6, CA-7 |

The opt-in profile is [compose.dev.yml](./compose.dev.yml), with instructions in
[README.md](./README.md#opt-in-hardened-development-deployment). It is a
development reference, not a universal production manifest. Kubernetes/OpenShift,
ECS, and VM-based runtimes need equivalent site-specific configuration.
RuntimeDefault seccomp, SELinux/AppArmor, service-account restrictions, and
platform admission controls should be applied where supported and validated
against the workload. No such orchestration controls are supplied by this image.

The [runtime audit](./README.md#read-only-runtime-configuration-and-drift-audit)
checks a running Docker container without changing it. Review each result and
the manifest before accepting a report as a baseline. Passing these narrow
checks does not complete the deployment checklist. Keep operational reports
in the site's controlled evidence repository, even though the audit excludes
environment values, secret contents, and host mount paths.

### Settings that must remain deployment inputs

Development may use the explicitly opted-in, per-deployment self-signed setup in
[TLS.md](./TLS.md). Replace it with the site's approved server identity before
operational use; verify its matching private key, chain, SAN, expiry, readiness
trust, and rotation procedure. This does not close CMVP module-validation gaps.

| Input | Existing interface or required implementation |
| --- | --- |
| Application image | `SEMOSS_IMAGE` is a Compose input; select a digest from the approved registry. |
| External URL and ingress | Set SEMOSS's actual absolute HTTPS redirect/base URL in the supported application configuration; new homes default to localhost. Review proxies, forwarded headers, WebSockets, upload limits, and DNS. No generic URL environment-variable override is implemented here. |
| TLS files | Compose inputs `SEMOSS_TLS_KEYSTORE` and `SEMOSS_TLS_PASSWORD` mount existing files. Other platforms must supply the equivalent paths from an approved source. Bind mounts do not repair permissions. |
| Readiness trust/name | Compose inputs `SEMOSS_READINESS_CA` and optional `SEMOSS_READINESS_URL`; the hostname must match the certificate and resolve to this instance. |
| Identity and authorization | Site IdP, authentication requirements, group/role mapping, administrators, service identities, and account lifecycle. Use documented SEMOSS integration, not invented environment placeholders. |
| Data and credentials | Storage class/path, ownership, encryption/KMS, backup destination, connector endpoints, trust roots, and supported secret-manager integration. |
| Network policy | Approved ingress sources and destinations for IdP, databases, AI services, DNS, time, audit, and management. Build-time package access is not automatically runtime access. |
| Operations | Resource sizes, audit destination, retention, alert routes, RPO/RTO, maintenance windows, vulnerability SLAs, and accepted exceptions. |

## 2. Evidence record

Make a separate controlled copy for each deployment. For **every checklist ID**,
record an owner, outcome, test date, evidence link, and reviewer. Allowed outcomes:
`not started`, `pass`, `fail`, `inherited`, or `approved not applicable`.
An unchecked item is not a pass. Inherited/not-applicable items need rationale,
the applicable responsibility agreement, and reviewer acceptance.

| Deployment record | Value to complete |
| --- | --- |
| System / mission owner / environment | |
| Classification/CUI scope and approved boundary | |
| Applicable CC SRG, STIG/SRG, baseline, overlays, and tailoring versions | |
| Platform/service authorization and inherited-control references | |
| AO / security assessor / operations owner | |
| Image digest / source commit / deployment manifest revision | |
| Host/runtime/orchestrator versions and assessed configuration | |
| Assessment date / next review / evidence repository | |
| Approved RPO/RTO, retention, patch deadlines, and session parameters | |
| Open findings / exception approvals / expiration dates | |

## 3. Before exposure or sensitive data

These are prerequisites even though the remaining sections are post-deployment
checks. Restrict initial access and use synthetic test data until acceptance.

- [ ] **P01 - Boundary and authorization.** Identify the exact hosting services,
  regions, external services, data types, and permitted data flows. Verify the
  applicable cloud-service authorization, customer responsibilities, and
  mission/system authorization path. GitHub/GHCR and Iron Bank ancestry do not
  establish authorization for operational data. Owner: mission/security.
  Evidence: approved boundary, SSP/control matrix, service authorization references.
- [ ] **P02 - Baseline and exceptions.** Select the current applicable controls
  and STIG/SRG checks; record organization-defined parameters. Assign each control
  to application, platform, provider, or organization. Owner: security.
  Evidence: tailored baseline and approved, time-bounded exception records.
- [ ] **P03 - Artifact acceptance.** Record the exact digest, provenance, SBOM,
  final-image findings, and dispositions. Apply the approved signing/admission
  process, if required by the selected policy. A successful development build
  or report-only scan is not a promotion approval. Owner: release/security.
  Evidence: approved artifact inventory and verification results.
- [ ] **P04 - Recovery preparation.** Identify existing volumes and take an
  application-consistent backup before migration. Define seed/init procedures
  for empty volumes and a rollback plan. Never overwrite existing homes or use
  volume-deleting commands as part of acceptance. Owner: operations/data.
  Evidence: storage inventory, backup record, reviewed migration/rollback procedure.
- [ ] **P05 - Isolated bootstrap.** Provision certificates, secret files, trust
  roots, administrator bootstrap, DNS, and initial access restrictions. Do not
  enable public registration to bypass account setup. Owner: application/platform.
  Evidence: reviewed configuration and access checks without recording secrets.
- [ ] **P06 - Cryptography evidence.** Record the unresolved FIPS module,
  runtime/OS, Python, storage, and external-service cryptography coverage.
  Do not mark this passed merely because remediation is deferred or self-tests
  pass. Obtain the evidence and authorization decisions required before
  protected-data use. Owner: security/cryptographic service owners.

## 4. Immediately after deployment: technical acceptance

Run read-only checks first. Use dedicated test accounts and disposable test
records for functional checks; coordinate failure/rotation tests so they cannot
disrupt shared or production services.

- [ ] **D01 - Identity and platform.** Compare the running image digest and
  manifest to the approved record. Confirm the supported architecture/CPU
  (current template: linux/amd64, UBI 10 x86-64-v3) and host/runtime patch and
  hardening status. Evidence: running inventory and applicable platform checks.
- [ ] **D02 - Isolation.** Inspect the effective non-root identity, capabilities,
  privilege-escalation prevention, root filesystem, mounts, namespace/device
  access, and platform security profiles. Verify only intended paths are writable.
  Evidence: effective runtime settings and permission checks.
- [ ] **D03 - Capacity and lifecycle.** Verify CPU/memory/PID/storage limits,
  headroom, and alerts. Confirm graceful shutdown and startup using a disposable
  instance; set startup tolerance to observed initialization time. Do not use
  backend readiness failure as an aggressive restart loop. Evidence: resource
  configuration and controlled lifecycle results.
- [ ] **D04 - Routing and trust.** Verify the actual external URL, redirects,
  certificate chain, hostname, trust roots, expiry, and ingress-to-backend
  protection. Confirm there is no unintended bypass/listener and no insecure
  certificate-verification exception. Evidence: synthetic connection tests and
  effective ingress configuration.
- [ ] **D05 - Backend readiness.** Verify CA/hostname-checked HTTP 200 and JSON
  `startupComplete: true` from this instance. Demonstrate rejection of wrong CA,
  wrong hostname, redirects, and non-ready responses in an isolated fixture.
  Kubernetes native HTTPS probes skip certificate verification; if verified TLS
  is required, use an appropriate exec/custom probe such as this helper rather
  than assuming `scheme: HTTPS` verifies identity. Evidence: probe config/results.
- [ ] **D06 - Application smoke test.** Confirm approved login, logout, CSRF
  handling, session expiry/cookies, basic Pixel calculation, Python execution,
  and required uploads using synthetic data. Test through the actual ingress
  and IdP, not only localhost. Evidence: redacted acceptance results.
- [ ] **D07 - Authentication.** Verify required enterprise SSO, MFA/CAC/PIV as
  applicable, account lockout/recovery, disabled-account rejection, and controlled
  break-glass access. Confirm bootstrap access is closed. Evidence: policy and
  positive/negative test-account results. Do not assume a local password login
  meets the selected identity requirements.
- [ ] **D08 - Authorization.** Test approved roles, administrator separation,
  least-privilege service identities, and denied access to another user's/project's
  data. Record owners for periodic access review. Evidence: role matrix and
  denied-action tests.
- [ ] **D09 - Secret handling.** Verify least-privilege retrieval, file permissions,
  supported connector secret integration, rotation/reload, and redacted logs.
  Check for credentials persisted in `.smss` files and address them according to
  approved policy; do not print their contents. Generic `${ENV}` substitution is
  not established by this template. Evidence: access/rotation results and design.
- [ ] **D10 - Network controls.** Verify approved routes, ingress sources,
  segmentation, administrative access, and runtime egress. Stage deny policies
  only after approved destinations are identified. Test both permitted and
  prohibited paths using harmless requests. Evidence: effective policies and tests.
- [ ] **D11 - Data protection.** Verify encryption/access controls for application
  volumes, databases, uploads, snapshots, and backups, including KMS permissions
  and region/residency requirements. A TLS keystore does not encrypt application
  data. Evidence: effective storage/KMS settings and ownership/access checks.
- [ ] **D12 - Logging and audit.** Verify collection of authentication failures,
  privileged actions, role/configuration changes, and required data/connector
  activity. Correlate identities and synchronized timestamps. Test centralized
  forwarding, access restriction, integrity protection, retention, and alert
  delivery. Local rotation is not an audit retention policy. Identify missing
  application events rather than claiming a logging switch supplies them.
  Evidence: redacted sample events, retention settings, and alert receipts.
- [ ] **D13 - Connectors and external services.** Validate required databases,
  model endpoints, and storage with test credentials and synthetic data.
  Verify server identity, least privilege, unauthorized-write denial, approved
  destination/data residency, and errors on failed trust/authentication.
  Evidence: connector matrix and positive/negative acceptance results.
- [ ] **D14 - Monitoring and drift.** Verify health, resource, certificate-expiry,
  failed backup/build, missing scan, and security alerts reach accountable owners.
  Establish read-only drift comparison to approved manifests; do not silently
  repair a running system. For Docker, use the optional runtime audit's
  `--baseline` comparison and explicitly select `--strict` if an automated check
  should fail on differences. This supplies neither scheduling nor alert delivery.
  Evidence: alert routing and configuration comparison.

## 5. Recovery, assessment, and operational acceptance

- [ ] **O01 - Restore demonstration.** Restore into an isolated new volume/system,
  measure RPO/RTO, and verify application data, ownership, configuration, and
  secret-recovery dependencies. A completed backup job is not a restore test.
  Evidence: timed restore report and data-owner acceptance.
- [ ] **O02 - Recovery and rollback.** Test upgrade/rollback compatibility with
  persistent data and image-digest selection. For required HA, verify failover
  and shared-state behavior; this template does not supply HA architecture.
  Evidence: approved exercise results.
- [ ] **O03 - Vulnerability and hardening assessment.** Assess the final deployed
  image, host, runtime, ingress, and applicable application STIG/SRG settings.
  Triage findings, false positives, remediation deadlines, and approved risk
  dispositions. Retain reports in the approved repository, not merely 14-day
  development artifacts. Evidence: assessment results and POA&M/dispositions.
- [ ] **O04 - Incident response and operations.** Assign on-call ownership,
  incident reporting/escalation, evidence preservation, access revocation,
  and isolation procedures. Exercise them safely. Evidence: runbooks and exercise.
- [ ] **O05 - Authorization decision.** Have the responsible assessors/AO review
  the SSP, inherited controls, technical evidence, open findings (including
  deferred FIPS work), and residual risk. Record the applicable authorization
  decision before operational sensitive-data use. A checked checklist is not an ATO.

## 6. Recurring maintenance

Use the approved frequencies and deadlines from the tailored baseline; the
examples below are workflow suggestions, not universal IL4 intervals.

- [ ] **R01 - Each image/configuration change:** rescan the exact candidate,
  review base and application dependency changes, record the new digest, and
  repeat affected acceptance checks before controlled promotion. Weekly base
  refreshes do not update every locked application dependency.
- [ ] **R02 - Scheduled monitoring:** confirm build/report/backup jobs actually
  run and alerts are delivered. At preparation, PR #3074 is open and the weekly
  GitHub schedule is not active. Verify its state rather than assuming activation.
- [ ] **R03 - Periodic review:** review access, service identities, secrets,
  certificates, network allowlists, drift, audit retention, and outstanding
  findings at site-approved intervals.
- [ ] **R04 - Recovery exercises:** repeat restore, rollback, and incident-response
  exercises at the approved cadence and after material architectural changes.
- [ ] **R05 - Boundary or policy changes:** reassess external services, data
  categories, regions, authorization inheritance, and relevant security evidence.

## References

- [Build and deployment guide](./README.md)
- [Optional Compose profile](./compose.dev.yml) and [readiness helper](./readiness.py)
- [Read-only Docker runtime audit](./audit_runtime.py)
- [JDBC acceptance guidance](./integrations/jdbc/README.md)
- [Bedrock acceptance guidance](./integrations/bedrock/README.md)
- [NIST SP 800-53 Rev. 5: control catalog](https://csrc.nist.gov/pubs/sp/800/53/r5/upd1/final)
- [NIST SP 800-53B: baselines and tailoring](https://csrc.nist.gov/pubs/sp/800/53/b/upd1/final)
- [NIST SP 800-53A Rev. 5: assessment procedures](https://csrc.nist.gov/pubs/sp/800/53/a/r5/final)
- [DISA cloud-security document library](https://www.cyber.mil/dccs/dccs-documents/)
- [Kubernetes HTTP probes and HTTPS verification behavior](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#http-probes)

Verify current publications and deployment-specific applicability with the
security team. Organizational, personnel, physical, acquisition, privacy, and
other inherited controls remain outside what container settings alone can satisfy.
