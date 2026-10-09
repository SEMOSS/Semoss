# BuildKit is required; use its bundled frontend instead of pulling a floating frontend image.
ARG MAVEN_IMAGE=registry1.dso.mil/ironbank/opensource/maven/maven-openjdk-25:3.9.16@sha256:ccf861d920d03c0134c8cffb090918a00c4b7c4ef3b04558735ca823540dc0fc
ARG UBI_IMAGE=registry1.dso.mil/ironbank/redhat/ubi/ubi10:10.2@sha256:12c8329ddef89d047c15f28e4eb7848698a560c5da8622ad527c2844186c6aa3
ARG PYTHON_IMAGE=registry1.dso.mil/ironbank/opensource/python:v3.14@sha256:eb433b924cccd4bb13068ff6284296e7c75a9a5dbc21b6bcd46bb9a38e71732d

FROM ${MAVEN_IMAGE} AS assembly
USER 0
WORKDIR /build
COPY artifacts.lock.json assemble.py audio_compat.py python_check.py refresh_bases.py pin_dev_artifacts.py ./
COPY tests/ tests/
RUN python3 -m unittest discover -s tests -v
# EXPERIMENTAL: built from SEMOSS/Monolith dev HEAD (FIPS-compliant PBKDF2 password hashing,
# commit 612f0c9, not yet in any published release). CI produces dev-build-artifacts/ with
# build-dev-artifacts.Dockerfile before this Dockerfile ever runs (see il4-container.yml's
# "Build dev-HEAD Monolith/semosshome artifacts" step); a local build must populate that
# directory the same way first. semossweb remains pinned to the published 5.4.0 release.
COPY dev-build-artifacts/ /build/dev-build-artifacts/
# Maven output isn't byte reproducible between builds of the same commit (javadoc
# timestamps, archive member ordering), so these 3 coordinates can't carry a fixed
# sha256 in artifacts.lock.json the way an externally fetched release artifact can;
# pin_dev_artifacts.py repins them to what was actually just built, immediately
# before assemble.py's resolve() enforces an exact match on every coordinate.
RUN python3 pin_dev_artifacts.py
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -ntp install:install-file \
      -Dfile=/build/dev-build-artifacts/semoss-0.0.1-SNAPSHOT-semosshome.tar.gz \
      -DgroupId=org.semoss -DartifactId=semoss -Dversion=0.0.1-SNAPSHOT \
      -Dclassifier=semosshome -Dpackaging=tar.gz && \
    mvn -B -ntp install:install-file \
      -Dfile=/build/dev-build-artifacts/monolith-0.0.1-SNAPSHOT.war \
      -DgroupId=org.semoss -DartifactId=monolith -Dversion=0.0.1-SNAPSHOT -Dpackaging=war && \
    mvn -B -ntp install:install-file \
      -Dfile=/build/dev-build-artifacts/monolith-0.0.1-SNAPSHOT-libraries.tar.gz \
      -DgroupId=org.semoss -DartifactId=monolith -Dversion=0.0.1-SNAPSHOT \
      -Dclassifier=libraries -Dpackaging=tar.gz
RUN --mount=type=cache,target=/root/.m2 \
    --mount=type=secret,id=maven_settings,target=/root/.m2/settings.xml \
    python3 assemble.py
# Dereference the RPM JDK's external truststore symlink before changing distributions.
RUN mkdir -p /out/java && cp -aL "${JAVA_HOME}/." /out/java/
COPY FipsCheck.java JdbcCheck.java JdbcTls.java /build/
RUN mkdir -p /out/fips-check && \
    javac -cp '/out/fips/*' -d /out/fips-check /build/FipsCheck.java /build/JdbcCheck.java /build/JdbcTls.java \
      /build/tests/JdbcTlsTest.java

FROM ${PYTHON_IMAGE} AS python-runtime
USER 0
RUN mkdir -p /out/usr/bin /out/usr/lib64 /out/opt/provenance && \
    cp -a /usr/bin/python3.14 /out/usr/bin/ && \
    cp -a /usr/lib64/libpython3.14.so.1.0 /usr/lib64/python3.14 /out/usr/lib64/ && \
    rpm -q python3.14 python3.14-libs > /out/opt/provenance/python-donor-rpms.txt

FROM python-runtime AS python-dependencies
RUN dnf -y --setopt=install_weak_deps=False install gcc-c++ python3.14-devel enchant2 \
    && dnf clean all
WORKDIR /build/python
COPY python-dependencies/ ./
ENV UV_PYTHON_DOWNLOADS=never
RUN --mount=type=cache,target=/build/python/.cache \
    VENV=/opt/semoss-python sh install.sh
# fsspec 2023.10.0 stays pinned (fsspec>=2026.6.0 needs datasets>=5.1.0, which
# needs pyarrow>=24.0.0, reopening the pyarrow/datasets Python 3.14 pin); this
# closes CVE-2026-104851 (GHSA-27vj-qcqg-25rc) directly instead. SEMOSS does
# not use fsspec's ReferenceFileSystem/Kerchunk implementation.
RUN --network=none /opt/semoss-python/bin/python3.14 fsspec_compat.py /opt/semoss-python
ADD --checksum=sha256:e57f64187974277726a3417ca6f181ec5403676c717672eef6a748a7b20e0106 \
    https://raw.githubusercontent.com/nltk/nltk_data/550b6625bcef1f2abff2ff770a5a0d272c9c6b2a/packages/tokenizers/punkt_tab.zip /build/punkt_tab.zip
RUN --network=none python3.14 -m zipfile -e /build/punkt_tab.zip /opt/nltk_data/tokenizers && \
    sha256sum /build/punkt_tab.zip > tokenizer-sha256.txt
RUN --network=none /opt/semoss-python/bin/python validate.py && \
    rpm -qa | sort > build-rpm-packages.txt

FROM ${UBI_IMAGE} AS runtime-base
USER 0
RUN dnf -y --setopt=install_weak_deps=False install \
        ca-certificates fontconfig freetype zlib libstdc++ tzdata \
        bzip2-libs expat libffi sqlite-libs readline xz-libs libuuid \
        openssl-libs libgomp gdbm-libs mpdecimal ncurses-libs libzstd enchant2 \
    && dnf clean all
COPY --from=python-runtime /out/ /
RUN --network=none python3.14 -c \
    "import ssl, bz2, lzma, ctypes, sqlite3, readline, decimal, curses, dbm.gnu, dbm.ndbm, compression.zstd, pyexpat, _uuid; print(ssl.OPENSSL_VERSION)"

FROM runtime-base AS runtime
ENV JAVA_HOME=/opt/java \
    CATALINA_HOME=/opt/tomcat \
    CATALINA_BASE=/opt/tomcat \
    TOMCAT_HOME=/opt/tomcat \
    HOME=/opt/semosshome \
    LANG=C.UTF-8 \
    VIRTUAL_ENV=/opt/semoss-python \
    PYTHONNOUSERSITE=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    NLTK_DATA=/opt/nltk_data \
    PATH=/opt/semoss-python/bin:/opt/java/bin:/opt/tomcat/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
COPY --from=python-dependencies /opt/semoss-python/ /opt/semoss-python/
COPY --from=python-dependencies /opt/nltk_data/ /opt/nltk_data/
COPY --from=python-dependencies /build/python/*.json /build/python/*.lock /build/python/*.toml /build/python/*.in /build/python/*.txt /opt/provenance/python/
COPY python_check.py /opt/python-check/python_check.py
COPY python-dependencies/validate.py /opt/python-check/validate.py
COPY tests/check_audio.py /opt/python-check/check_audio.py
COPY --from=assembly /out/java/ /opt/java/
COPY --from=assembly /out/tomcat/ /opt/tomcat/
COPY --from=assembly /out/fips/ /opt/fips/
COPY --from=assembly /out/fips-check/ /opt/fips-check/
COPY --from=assembly /out/sqlite/ /opt/sqlite/
COPY --from=assembly /out/provenance/ /opt/provenance/
COPY --from=assembly --chown=10001:10001 /out/semosshome/ /opt/semosshome/
COPY conf/server.xml conf/context.xml conf/logging.properties /opt/tomcat/conf/
COPY conf/setenv.sh /opt/tomcat/bin/setenv.sh
COPY entrypoint.sh /usr/local/bin/semoss-entrypoint
COPY RdsTrust.java tests/RdsTrustTest.java /opt/rds-check/
# Compiled into Tomcat's own lib/, not the application WAR: CsrfPreventionFilterBase's
# single-argument Class.forName(randomClass) resolves using ITS OWN (Tomcat's shared)
# classloader, which has no visibility into any webapp's WEB-INF/lib - confirmed by
# reproducing the resulting ClassNotFoundException with the class bundled in the WAR.
COPY FipsPreferredSecureRandom.java /opt/tomcat/lib/
ADD --checksum=sha256:cffb079b116f6bb731b10b1dc3e026287758b0c2fdc81144e218da0a58650454 \
    https://truststore.pki.us-gov-west-1.rds.amazonaws.com/us-gov-west-1/us-gov-west-1-bundle.pem /opt/semoss-ca/us-gov-west-1-bundle.pem
ADD --checksum=sha256:701cd34b338d4199cd09a669bbc3d11000febe52393d08a689d0b21c50ea3974 \
    https://truststore.pki.us-gov-west-1.rds.amazonaws.com/us-gov-east-1/us-gov-east-1-bundle.pem /opt/semoss-ca/us-gov-east-1-bundle.pem

RUN java -version && \
    keytool -importkeystore -noprompt \
      -srckeystore /opt/java/lib/security/cacerts -srcstorepass changeit \
      -destkeystore /opt/fips/cacerts.bcfks -deststoretype BCFKS \
      -deststorepass changeit -providername BCFIPS \
      -providerclass org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider \
      -providerpath /opt/fips/bc-fips-2.1.3.jar && \
    sed '/^security\.provider\.[0-9]*=/d; /^ssl\.KeyManagerFactory\.algorithm=/d; /^ssl\.TrustManagerFactory\.algorithm=/d; /^keystore\.type=/d' \
      /opt/java/conf/security/java.security > /opt/fips/java.security && \
    printf '%s\n' \
      'security.provider.1=org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider' \
      'security.provider.2=org.bouncycastle.jsse.provider.BouncyCastleJsseProvider fips:BCFIPS' \
      'security.provider.3=SUN' \
      'ssl.KeyManagerFactory.algorithm=PKIX' \
      'ssl.TrustManagerFactory.algorithm=PKIX' \
      'jdk.tls.disabledAlgorithms=SSLv3, TLSv1, TLSv1.1, DTLSv1.0, RC4, DES, MD5withRSA, DH keySize < 2048, EC keySize < 224, 3DES_EDE_CBC, anon, NULL, ECDH, SHA1, DSA, DHE, CBC, CHACHA20-POLY1305, TLS_RSA_WITH_AES_256_GCM_SHA384, TLS_RSA_WITH_AES_128_GCM_SHA256, TLS_RSA_WITH_AES_256_CBC_SHA256, TLS_RSA_WITH_AES_128_CBC_SHA256, X25519, X448, Ed25519, Ed448' \
      'jdk.certpath.disabledAlgorithms=MD2, MD5, SHA1, RSA keySize < 2048, DSA, EC keySize < 224' \
      'keystore.type=BCFKS' >> /opt/fips/java.security && \
    chmod 0555 /usr/local/bin/semoss-entrypoint && \
    mkdir -p /opt/tomcat/work /opt/tomcat/temp /opt/tomcat/logs \
      /opt/tomcat/conf/Catalina/localhost && \
    chown -R 10001:10001 /opt/tomcat/work /opt/tomcat/temp /opt/tomcat/logs && \
    rpm -qa | sort > /opt/provenance/rpm-packages.txt

RUN --network=none javac -d /opt/tomcat/lib /opt/tomcat/lib/FipsPreferredSecureRandom.java
RUN --network=none javac -d /opt/rds-check /opt/rds-check/RdsTrust.java /opt/rds-check/RdsTrustTest.java && \
    java -cp /opt/rds-check RdsTrustTest /opt/semoss-ca/us-gov-west-1-bundle.pem /opt/semoss-ca/us-gov-east-1-bundle.pem && \
    java -Dorg.bouncycastle.native.cpu_variant=java -cp '/opt/rds-check:/opt/fips/*' RdsTrust /opt/fips/cacerts.bcfks BCFKS \
      /opt/semoss-ca/us-gov-west-1-bundle.pem /opt/semoss-ca/us-gov-east-1-bundle.pem && \
    chmod 0644 /opt/semoss-ca/* && \
    cp /opt/semoss-ca/roots-sha256.txt /opt/provenance/aws-rds-govcloud-roots.txt

USER 10001:10001
WORKDIR /opt/semosshome
RUN /usr/local/bin/semoss-entrypoint --check
RUN --network=none . /opt/tomcat/bin/setenv.sh && \
    java $CATALINA_OPTS -cp '/opt/fips/*:/opt/fips-check:/opt/tomcat/webapps/Monolith/WEB-INF/lib/*' JdbcTlsTest
RUN --network=none /opt/semoss-python/bin/python /opt/python-check/python_check.py
RUN --network=none cd /tmp && /opt/semoss-python/bin/python /opt/python-check/check_audio.py && \
    rm /tmp/pipecat.log
LABEL org.opencontainers.image.title="SEMOSS 5.4.0 UBI10 BC FIPS template" \
      org.opencontainers.image.version="5.4.0" \
      org.opencontainers.image.source="https://github.com/SEMOSS/Semoss" \
      org.opencontainers.image.revision="565b2c1825011304a93eabcb21cf2cd33135a42a"
EXPOSE 8443
ENTRYPOINT ["/usr/local/bin/semoss-entrypoint"]
CMD ["run"]
