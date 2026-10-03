# Builds the SEMOSS/Monolith dev-HEAD artifacts consumed by Dockerfile's
# dev-build-artifacts/ COPY step. Run from the repo root (not docker/il4),
# with a `monolith-src` named build context pointing at a checkout of
# SEMOSS/Monolith at the commit pinned in il4-container.yml:
#
#   docker buildx build -f docker/il4/build-dev-artifacts.Dockerfile \
#     --build-arg MAVEN_IMAGE="$MAVEN_IMAGE" \
#     --build-context monolith-src=/path/to/monolith/checkout \
#     --target artifacts --output type=local,dest=docker/il4/dev-build-artifacts .
#
# No --mount=type=cache here: a cache mount's contents don't survive into the
# stage's own filesystem, so a later COPY --from=<stage> can't see anything
# written under it. monolith-build depends on semoss-build's whole /root/.m2
# (not just its own fresh one) because Monolith's pom resolves
# org.semoss:semoss:0.0.1-SNAPSHOT as a dependency, and that SNAPSHOT is
# never published anywhere external.
ARG MAVEN_IMAGE

FROM ${MAVEN_IMAGE} AS semoss-build
USER 0
WORKDIR /build
COPY . .
RUN --mount=type=secret,id=maven_settings,target=/root/.m2/settings.xml \
    mvn -B -ntp -P deploy clean install -DskipTests -Dgpg.skip=true

FROM ${MAVEN_IMAGE} AS monolith-build
USER 0
WORKDIR /build
COPY --from=semoss-build /root/.m2 /root/.m2
COPY --from=monolith-src . .
RUN --mount=type=secret,id=maven_settings,target=/root/.m2/settings.xml \
    mvn -B -ntp -P deploy,fips clean install -DskipTests -Dgpg.skip=true

FROM scratch AS artifacts
COPY --from=semoss-build /root/.m2/repository/org/semoss/semoss/0.0.1-SNAPSHOT/semoss-0.0.1-SNAPSHOT-semosshome.tar.gz /
COPY --from=monolith-build /root/.m2/repository/org/semoss/monolith/0.0.1-SNAPSHOT/monolith-0.0.1-SNAPSHOT.war /
COPY --from=monolith-build /root/.m2/repository/org/semoss/monolith/0.0.1-SNAPSHOT/monolith-0.0.1-SNAPSHOT-libraries.tar.gz /
