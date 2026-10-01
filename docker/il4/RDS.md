# AWS GovCloud RDS and Aurora certificate trust

Both image variants add the six current, explicitly pinned AWS RDS G1 roots for
`us-gov-west-1` and `us-gov-east-1`: RSA2048, RSA4096, and ECC384 per region.
Existing JDK public roots are preserved. AWS intermediate/leaf certificates are
not installed as trust anchors. This supports valid RDS/Aurora server chains,
not unconditional trust of a hostname suffix.

## Build-time trust and provenance

[Dockerfile](./Dockerfile) retrieves both bundles over HTTPS from the
[AWS-documented GovCloud truststore](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.SSL.html).
Both regional downloads use `truststore.pki.us-gov-west-1.rds.amazonaws.com`.
BuildKit enforces the reviewed SHA-256 hashes:

| Region | Regional bundle SHA-256 |
| --- | --- |
| us-gov-west-1 | `cffb079b116f6bb731b10b1dc3e026287758b0c2fdc81144e218da0a58650454` |
| us-gov-east-1 | `701cd34b338d4199cd09a669bbc3d11000febe52393d08a689d0b21c50ea3974` |

[RdsTrust.java](./RdsTrust.java) requires exactly six reviewed root fingerprints,
valid dates, CA/key-signing constraints and valid self-signatures. Missing,
duplicate or unknown roots fail the build. Intermediate certificates are excluded.
There is no startup download, automatic expansion of trust, or endpoint probing.

| Consumer | Trust material |
| --- | --- |
| BC Java/JDBC | Existing `/opt/fips/cacerts.bcfks`, plus the six roots |
| ACCP Java/JDBC | Existing `/opt/accp/cacerts.p12`, plus the six roots |
| Explicit PEM clients | `/opt/semoss-ca/aws-rds-govcloud.pem`, containing only the six roots |
| Evidence | `/opt/provenance/aws-rds-govcloud-roots.txt`, with root fingerprints and preserved-entry count |

Adding public CA certificates does not add credentials or change networking.
It does not establish FIPS validation, IL4 authorization, RDS IAM authentication,
or acceptance of any particular production database.

## PostgreSQL / Aurora PostgreSQL

Use the actual AWS cluster/instance endpoint, not an IP address or an arbitrary
DNS alias. In SEMOSS, use `RDBMS_TYPE=POSTGRES` and this `CONNECTION_URL` shape:

```text
jdbc:postgresql://example.cluster-EXAMPLE.us-gov-west-1.rds.amazonaws.com:5432/application?sslmode=verify-full&sslfactory=org.postgresql.ssl.DefaultJavaSSLFactory&connectTimeout=10&socketTimeout=30
```

`DefaultJavaSSLFactory` selects the image's Java truststore, which now includes
the GovCloud RDS roots. `sslmode=verify-full` requires certificate-chain and
hostname validation. Keep credentials separate from the URL.

This is not global rewriting/enforcement of user-entered JDBC URLs. pgjdbc's
default PEM-based SSL factory does not automatically use the Java truststore.
For that factory, explicitly use:

```text
jdbc:postgresql://example.cluster-EXAMPLE.us-gov-west-1.rds.amazonaws.com:5432/application?sslmode=verify-full&sslrootcert=/opt/semoss-ca/aws-rds-govcloud.pem&connectTimeout=10&socketTimeout=30
```

Python/libpq clients can use the same PEM with `sslmode=verify-full` and
`sslrootcert=/opt/semoss-ca/aws-rds-govcloud.pem`. This does not change Python's
cryptographic validation status.

## Other JDBC clients and Bedrock

Use the reviewed MariaDB/SQL Server profiles in the
[JDBC guide](./integrations/jdbc/README.md); their explicit Java truststores receive
the same root additions. Do not set `trustServerCertificate=true`,
`sslMode=trust`, or disable hostname verification.

The Python SDK's public `certifi` bundle and existing Java public roots remain
unchanged. Bedrock uses those public CAs, not RDS's private CA hierarchy.
**Do not set `AWS_CA_BUNDLE` or `REQUESTS_CA_BUNDLE` to the RDS-only PEM.**
See [Bedrock trust and access requirements](./integrations/bedrock/README.md).

## Deployment, rotation, and limitations

- New trust requires deploying a newly built image. Previously published image
  digests do not change when repository sources change.
- A mounted replacement Java truststore hides the image's store. Include the
  approved RDS roots in that custom store and validate the applicable startup
  trust-inventory checks; do not assume image updates modify mounted secrets.
- Do not import RDS server/intermediate certificates as roots. Normal server
  rotation remains valid when the chain leads to a reviewed root.
- If AWS updates a regional bundle, a clean build fails its checksum until the
  new bundle is reviewed. Update hashes, root pins when needed, tests, and
  provenance deliberately. Weekly rebuilding is not permission to trust new CAs.
- Verify actual RDS CA identifiers, DNS, VPC routes/security groups, database
  credentials and least-privilege access in GovCloud. Require SSL server-side.
- Test the actual deployment with approved test credentials, plus negative
  wrong-CA/hostname/password checks. Local fixture tests and root import checks
  do not claim a connection to a customer's RDS endpoint.
