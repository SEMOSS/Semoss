# SSHJ algorithm inventory and FIPS negotiation policy

This inventory is for **SSHJ 0.41.0**, the version used by this branch. It covers
all 16 `KeyAlgorithms` factories, all 33 cipher factories in `BlockCiphers`,
`GcmCiphers`, `ChachaPolyCiphers` and `StreamCiphers`, all 16 `Macs` factories,
and all 23 key-exchange/extension factories in `DefaultConfig`. Deprecated
aliases for the same protocol names are not duplicated. Public factories that
are not offered by the default configuration are included. This is a complete
inventory of those versioned APIs, not of every possible SSH extension.

The names were extracted from the installed JAR's public factories and checked
against the [SSHJ 0.41.0 source JAR](https://repo.maven.apache.org/maven2/com/hierynomus/sshj/0.41.0/sshj-0.41.0-sources.jar).
Revisit the inventory when upgrading SSHJ. New dependency algorithms are not
silently added to the explicit FIPS policy.

## Runtime behavior

`SFTPStorageEngine.createSshConfig()` returns SSHJ defaults in standard mode.
If either `org.bouncycastle.fips.approved_only=true` or `SEMOSS_FIPS=true`, it
requires BCFIPS to be registered first and the calling thread to actually be in
approved-only mode, then installs the explicit lists below. An invalid runtime
or no common algorithm fails the connection; there is no unrestricted retry.
The image must still provide the correct provider libraries and JVM settings.

FIPS mode enables 16 real key-exchange names plus `ext-info-c`, six plain
host-signature algorithms, three AES-CTR ciphers and four SHA-2 MACs. RSA host
keys should have at least 2048 bits. Password authentication still uses the
server's host key. Provider registration and approved-only mode are not changed
by this method. The existing host-key verifier is outside this change.

The basis for algorithm selection is the
[BC-FJA 2.1.1 security policy, Table 4 and section 2.2](https://csrc.nist.gov/CSRC/media/projects/cryptographic-module-validation-program/documents/security-policies/140sp4943.pdf),
with actual compatibility checks against the image's **bc-fips 2.1.3** runtime.
A table entry marked **Enabled** describes our negotiation policy, not a separate
FIPS validation of SSHJ or proof of certificate coverage for a different binary.
An **Excluded** entry can be a deliberate policy restriction even when its
underlying primitive is approved.

## Host signatures (16)

| SSH name | SSHJ factory | FIPS configuration | Reason / verification |
| --- | --- | --- | --- |
| `ecdsa-sha2-nistp256` | `KeyAlgorithms.ECDSASHANistp256()` | Enabled | RSA SHA-2, NIST ECDSA, or Ed25519; forced host-signature transfer passed. |
| `ecdsa-sha2-nistp256-cert-v01@openssh.com` | `KeyAlgorithms.ECDSASHANistp256CertV01()` | Excluded | Certificate/CA-signature path not covered by this policy. |
| `ecdsa-sha2-nistp384` | `KeyAlgorithms.ECDSASHANistp384()` | Enabled | RSA SHA-2, NIST ECDSA, or Ed25519; forced host-signature transfer passed. |
| `ecdsa-sha2-nistp384-cert-v01@openssh.com` | `KeyAlgorithms.ECDSASHANistp384CertV01()` | Excluded | Certificate/CA-signature path not covered by this policy. |
| `ecdsa-sha2-nistp521` | `KeyAlgorithms.ECDSASHANistp521()` | Enabled | RSA SHA-2, NIST ECDSA, or Ed25519; forced host-signature transfer passed. |
| `ecdsa-sha2-nistp521-cert-v01@openssh.com` | `KeyAlgorithms.ECDSASHANistp521CertV01()` | Excluded | Certificate/CA-signature path not covered by this policy. |
| `rsa-sha2-256` | `KeyAlgorithms.RSASHA256()` | Enabled | RSA SHA-2, NIST ECDSA, or Ed25519; forced host-signature transfer passed. |
| `rsa-sha2-512` | `KeyAlgorithms.RSASHA512()` | Enabled | RSA SHA-2, NIST ECDSA, or Ed25519; forced host-signature transfer passed. |
| `sk-ecdsa-sha2-nistp256@openssh.com` | `KeyAlgorithms.SkECDSANistp256()` | Excluded | Security-key integration not covered by this policy. |
| `sk-ssh-ed25519@openssh.com` | `KeyAlgorithms.SkSSHEd25519()` | Excluded | Security-key integration not covered by this policy. |
| `ssh-dss` | `KeyAlgorithms.SSHDSA()` | Excluded | Legacy SHA-1 signature policy (and 1024-bit DSA for ssh-dss). |
| `ssh-dss-cert-v01@openssh.com` | `KeyAlgorithms.SSHDSSCertV01()` | Excluded | Certificate/CA-signature path not covered by this policy. |
| `ssh-ed25519` | `KeyAlgorithms.EdDSA25519()` | Enabled | RSA SHA-2, NIST ECDSA, or Ed25519; forced host-signature transfer passed. |
| `ssh-ed25519-cert-v01@openssh.com` | `KeyAlgorithms.EdDSA25519CertV01()` | Excluded | Certificate/CA-signature path not covered by this policy. |
| `ssh-rsa` | `KeyAlgorithms.SSHRSA()` | Excluded | Legacy SHA-1 signature policy (and 1024-bit DSA for ssh-dss). |
| `ssh-rsa-cert-v01@openssh.com` | `KeyAlgorithms.SSHRSACertV01()` | Excluded | Certificate/CA-signature path not covered by this policy. |

## Ciphers (33)

| SSH name | SSHJ factory | FIPS configuration | Reason / verification |
| --- | --- | --- | --- |
| `3des-cbc` | `BlockCiphers.TripleDESCBC()` | Excluded | Legacy cipher; not selected for new SSH encryption. |
| `3des-ctr` | `BlockCiphers.TripleDESCTR()` | Excluded | Legacy cipher; not selected for new SSH encryption. |
| `aes128-cbc` | `BlockCiphers.AES128CBC()` | Excluded | Approved AES primitive; this SSH policy chooses CTR instead of legacy CBC. |
| `aes128-ctr` | `BlockCiphers.AES128CTR()` | Enabled | Forced cipher transfer and rekey passed. |
| `aes128-gcm@openssh.com` | `GcmCiphers.AES128GCM()` | Excluded | SSHJ generates/imports IVs outside the module; see the GCM limitation below. |
| `aes192-cbc` | `BlockCiphers.AES192CBC()` | Excluded | Approved AES primitive; this SSH policy chooses CTR instead of legacy CBC. |
| `aes192-ctr` | `BlockCiphers.AES192CTR()` | Enabled | Forced cipher transfer and rekey passed. |
| `aes256-cbc` | `BlockCiphers.AES256CBC()` | Excluded | Approved AES primitive; this SSH policy chooses CTR instead of legacy CBC. |
| `aes256-ctr` | `BlockCiphers.AES256CTR()` | Enabled | Forced cipher transfer and rekey passed. |
| `aes256-gcm@openssh.com` | `GcmCiphers.AES256GCM()` | Excluded | SSHJ generates/imports IVs outside the module; see the GCM limitation below. |
| `arcfour` | `StreamCiphers.Arcfour()` | Excluded | Outside the approved cipher set selected for this BCFIPS deployment. |
| `arcfour128` | `StreamCiphers.Arcfour128()` | Excluded | Outside the approved cipher set selected for this BCFIPS deployment. |
| `arcfour256` | `StreamCiphers.Arcfour256()` | Excluded | Outside the approved cipher set selected for this BCFIPS deployment. |
| `blowfish-cbc` | `BlockCiphers.BlowfishCBC()` | Excluded | Outside the approved cipher set selected for this BCFIPS deployment. |
| `blowfish-ctr` | `BlockCiphers.BlowfishCTR()` | Excluded | Outside the approved cipher set selected for this BCFIPS deployment. |
| `cast128-cbc` | `BlockCiphers.Cast128CBC()` | Excluded | Outside the approved cipher set selected for this BCFIPS deployment. |
| `cast128-ctr` | `BlockCiphers.Cast128CTR()` | Excluded | Outside the approved cipher set selected for this BCFIPS deployment. |
| `chacha20-poly1305@openssh.com` | `ChachaPolyCiphers.CHACHA_POLY_OPENSSH()` | Excluded | Outside the approved cipher set selected for this BCFIPS deployment. |
| `idea-cbc` | `BlockCiphers.IDEACBC()` | Excluded | Outside the approved cipher set selected for this BCFIPS deployment. |
| `idea-ctr` | `BlockCiphers.IDEACTR()` | Excluded | Outside the approved cipher set selected for this BCFIPS deployment. |
| `serpent128-cbc` | `BlockCiphers.Serpent128CBC()` | Excluded | Outside the approved cipher set selected for this BCFIPS deployment. |
| `serpent128-ctr` | `BlockCiphers.Serpent128CTR()` | Excluded | Outside the approved cipher set selected for this BCFIPS deployment. |
| `serpent192-cbc` | `BlockCiphers.Serpent192CBC()` | Excluded | Outside the approved cipher set selected for this BCFIPS deployment. |
| `serpent192-ctr` | `BlockCiphers.Serpent192CTR()` | Excluded | Outside the approved cipher set selected for this BCFIPS deployment. |
| `serpent256-cbc` | `BlockCiphers.Serpent256CBC()` | Excluded | Outside the approved cipher set selected for this BCFIPS deployment. |
| `serpent256-ctr` | `BlockCiphers.Serpent256CTR()` | Excluded | Outside the approved cipher set selected for this BCFIPS deployment. |
| `twofish-cbc` | `BlockCiphers.TwofishCBC()` | Excluded | Outside the approved cipher set selected for this BCFIPS deployment. |
| `twofish128-cbc` | `BlockCiphers.Twofish128CBC()` | Excluded | Outside the approved cipher set selected for this BCFIPS deployment. |
| `twofish128-ctr` | `BlockCiphers.Twofish128CTR()` | Excluded | Outside the approved cipher set selected for this BCFIPS deployment. |
| `twofish192-cbc` | `BlockCiphers.Twofish192CBC()` | Excluded | Outside the approved cipher set selected for this BCFIPS deployment. |
| `twofish192-ctr` | `BlockCiphers.Twofish192CTR()` | Excluded | Outside the approved cipher set selected for this BCFIPS deployment. |
| `twofish256-cbc` | `BlockCiphers.Twofish256CBC()` | Excluded | Outside the approved cipher set selected for this BCFIPS deployment. |
| `twofish256-ctr` | `BlockCiphers.Twofish256CTR()` | Excluded | Outside the approved cipher set selected for this BCFIPS deployment. |

## Key exchanges (23)

| SSH name | SSHJ factory | FIPS configuration | Reason / verification |
| --- | --- | --- | --- |
| `curve25519-sha256` | `Curve25519SHA256.Factory` | Excluded | X25519 key exchange unavailable in this approved-only runtime. |
| `curve25519-sha256@libssh.org` | `Curve25519SHA256.FactoryLibSsh` | Excluded | X25519 key exchange unavailable in this approved-only runtime. |
| `diffie-hellman-group-exchange-sha1` | `DHGexSHA1.Factory` | Excluded | Server-selected domain; SSHJ accepts a 1024-bit minimum. Fixed reviewed groups are used instead. |
| `diffie-hellman-group-exchange-sha256` | `DHGexSHA256.Factory` | Excluded | Server-selected domain; SSHJ accepts a 1024-bit minimum. Fixed reviewed groups are used instead. |
| `diffie-hellman-group1-sha1` | `DHGroups.Group1SHA1()` | Excluded | Legacy SHA-1 KEX policy; group 1 also has a 1024-bit modulus. |
| `diffie-hellman-group14-sha1` | `DHGroups.Group14SHA1()` | Excluded | Legacy SHA-1 KEX policy; group 1 also has a 1024-bit modulus. |
| `diffie-hellman-group14-sha256` | `DHGroups.Group14SHA256()` | Enabled | Forced key-exchange transfer and rekey passed. |
| `diffie-hellman-group14-sha256@ssh.com` | `ExtendedDHGroups.Group14SHA256AtSSH()` | Enabled | Fixed MODP group and SHA-2; group agreement tested, this wire name not tested with a server. |
| `diffie-hellman-group15-sha256` | `ExtendedDHGroups.Group15SHA256()` | Enabled | Fixed MODP group and SHA-2; group agreement tested, this wire name not tested with a server. |
| `diffie-hellman-group15-sha256@ssh.com` | `ExtendedDHGroups.Group15SHA256AtSSH()` | Enabled | Fixed MODP group and SHA-2; group agreement tested, this wire name not tested with a server. |
| `diffie-hellman-group15-sha384@ssh.com` | `ExtendedDHGroups.Group15SHA384AtSSH()` | Enabled | Fixed MODP group and SHA-2; group agreement tested, this wire name not tested with a server. |
| `diffie-hellman-group15-sha512` | `DHGroups.Group15SHA512()` | Enabled | Fixed MODP group and SHA-2; group agreement tested, this wire name not tested with a server. |
| `diffie-hellman-group16-sha256` | `ExtendedDHGroups.Group16SHA256()` | Enabled | Fixed MODP group and SHA-2; group agreement tested, this wire name not tested with a server. |
| `diffie-hellman-group16-sha384@ssh.com` | `ExtendedDHGroups.Group16SHA384AtSSH()` | Enabled | Fixed MODP group and SHA-2; group agreement tested, this wire name not tested with a server. |
| `diffie-hellman-group16-sha512` | `DHGroups.Group16SHA512()` | Enabled | Forced key-exchange transfer and rekey passed. |
| `diffie-hellman-group16-sha512@ssh.com` | `ExtendedDHGroups.Group16SHA512AtSSH()` | Enabled | Fixed MODP group and SHA-2; group agreement tested, this wire name not tested with a server. |
| `diffie-hellman-group17-sha512` | `DHGroups.Group17SHA512()` | Enabled | Fixed MODP group and SHA-2; group agreement tested, this wire name not tested with a server. |
| `diffie-hellman-group18-sha512` | `DHGroups.Group18SHA512()` | Enabled | Forced key-exchange transfer and rekey passed. |
| `diffie-hellman-group18-sha512@ssh.com` | `ExtendedDHGroups.Group18SHA512AtSSH()` | Enabled | Fixed MODP group and SHA-2; group agreement tested, this wire name not tested with a server. |
| `ecdh-sha2-nistp256` | `ECDHNistP.Factory256` | Enabled | Forced key-exchange transfer and rekey passed. |
| `ecdh-sha2-nistp384` | `ECDHNistP.Factory384` | Enabled | Forced key-exchange transfer and rekey passed. |
| `ecdh-sha2-nistp521` | `ECDHNistP.Factory521` | Enabled | Forced key-exchange transfer and rekey passed. |
| `ext-info-c` | `ExtInfoClientFactory` | Retained | RFC 8308 extension signal, not a cryptographic algorithm. |

## MACs (16)

| SSH name | SSHJ factory | FIPS configuration | Reason / verification |
| --- | --- | --- | --- |
| `hmac-md5` | `Macs.HMACMD5()` | Excluded | MD5/RIPEMD MAC outside the approved set selected here. |
| `hmac-md5-96` | `Macs.HMACMD596()` | Excluded | MD5/RIPEMD MAC outside the approved set selected here. |
| `hmac-md5-96-etm@openssh.com` | `Macs.HMACMD596Etm()` | Excluded | MD5/RIPEMD MAC outside the approved set selected here. |
| `hmac-md5-etm@openssh.com` | `Macs.HMACMD5Etm()` | Excluded | MD5/RIPEMD MAC outside the approved set selected here. |
| `hmac-ripemd160` | `Macs.HMACRIPEMD160()` | Excluded | MD5/RIPEMD MAC outside the approved set selected here. |
| `hmac-ripemd160-96` | `Macs.HMACRIPEMD16096()` | Excluded | MD5/RIPEMD MAC outside the approved set selected here. |
| `hmac-ripemd160-etm@openssh.com` | `Macs.HMACRIPEMD160Etm()` | Excluded | MD5/RIPEMD MAC outside the approved set selected here. |
| `hmac-ripemd160@openssh.com` | `Macs.HMACRIPEMD160OpenSsh()` | Excluded | MD5/RIPEMD MAC outside the approved set selected here. |
| `hmac-sha1` | `Macs.HMACSHA1()` | Excluded | Policy selects SHA-2; this is not a blanket claim that HMAC-SHA-1 is unapproved. |
| `hmac-sha1-96` | `Macs.HMACSHA196()` | Excluded | Policy selects SHA-2; this is not a blanket claim that HMAC-SHA-1 is unapproved. |
| `hmac-sha1-96@openssh.com` | `Macs.HMACSHA196Etm()` | Excluded | Policy selects SHA-2; this is not a blanket claim that HMAC-SHA-1 is unapproved. |
| `hmac-sha1-etm@openssh.com` | `Macs.HMACSHA1Etm()` | Excluded | Policy selects SHA-2; this is not a blanket claim that HMAC-SHA-1 is unapproved. |
| `hmac-sha2-256` | `Macs.HMACSHA2256()` | Enabled | SHA-2 MAC; forced MAC transfer and rekey passed. |
| `hmac-sha2-256-etm@openssh.com` | `Macs.HMACSHA2256Etm()` | Enabled | SHA-2 MAC; forced MAC transfer and rekey passed. |
| `hmac-sha2-512` | `Macs.HMACSHA2512()` | Enabled | SHA-2 MAC; forced MAC transfer and rekey passed. |
| `hmac-sha2-512-etm@openssh.com` | `Macs.HMACSHA2512Etm()` | Enabled | SHA-2 MAC; forced MAC transfer and rekey passed. |

`Macs.HMACSHA196Etm()` reports `hmac-sha1-96@openssh.com` in SSHJ 0.41.0;
the inventory preserves that literal factory name. SHA-1 MACs remain excluded.
SSHJ also handles strict-KEX signaling internally; this configuration does not
disable it. `ext-info-c` is retained for extension negotiation.

## Constraints beyond the names

- **Ed25519 is not X25519.** Ed25519 signatures worked through BCFIPS in the
  forced host-key test. The two Curve25519/X25519 key-exchange names failed in
  the earlier default-client test and are excluded.
- **AES-GCM needs different integration.** SSHJ's `KeyExchanger.gotNewKeys()`
  derives an IV outside BCFIPS and `GcmCipher` passes it to JCE, then increments
  its counter outside the module. Security-policy section 2.2 disallows importing
  such an external encryption IV. Passing a generic AES-GCM check does not
  establish conformity of this SSH path; both SSHJ GCM factories are excluded.
- **SSH key derivation remains a compliance question.** SSHJ's `KeyExchanger`
  constructs the RFC 4253 derivation itself from digest calls rather than using
  BCFIPS's [SSH KDF service](https://downloads.bouncycastle.org/fips-java/docs/bc-fips-2.1.0-javadoc/org/bouncycastle/crypto/fips/FipsKDF.html).
  A successful transfer with this allowlist does not resolve that module-boundary
  question. Provider-version and Java-environment validation coverage also need
  their own evidence before making a full FIPS claim.
- **Certificate and security-key formats are separately excluded.** An ECDSA or
  Ed25519 certificate name alone does not establish the certificate/CA signature
  path. These formats were not included in the integration tests. Their exclusion
  is not a claim that ECDSA or Ed25519 is unapproved.

## Verification

The compiled application method was tested in local image
`sha256:0c7c751ecae5c79175883bd5cec7202a91ddc728b0472549ea8d4a0ff3a6f429`,
with Zulu 25.0.3, BCFIPS 2.1.3, BCJSSE 2.1.24 and approved-only mode.
An isolated OpenSSH SFTP fixture had no published ports and its public host keys
were pinned in the test client. Each of 19 forced-algorithm cases uploaded a
64 KiB file, rekeyed, downloaded and compared it, and deleted the remote file:

- Three NIST ECDH curves and fixed DH groups 14, 16 and 18.
- RSA SHA-256/SHA-512, ECDSA P-256/P-384/P-521 and Ed25519 host signatures.
- AES-128/192/256-CTR.
- HMAC-SHA-256/SHA-512, with and without OpenSSH encrypt-then-MAC.

Separate SSHJ/BCFIPS shared-secret agreement checks passed for MODP 2048, 3072,
4096, 6144 and 8192. The fixture does not implement the group-15/group-17 or
vendor-specific wire names; those names use the same reviewed `DHG` path and
fixed parameters, but have not been tested against a matching server. The tests
vary one algorithm category at a time, not every possible Cartesian combination.

Regression tests run standard and FIPS cases in fresh JVMs to avoid irreversible
provider/approved-mode state leaking between tests:

```sh
mvn -Dtest=SFTPStorageEngineSshConfigUnitTests test
```

They check unchanged standard defaults, approved-only provider selection, and
rejection of missing, reordered or non-approved BCFIPS configurations.
All six regression cases passed on Java 21. The non-FIPS local Docker image
also preserved the default algorithm lists and passed upload, rekey, download,
content comparison and deletion using the same compiled application method.
