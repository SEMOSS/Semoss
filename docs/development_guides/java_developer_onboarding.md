# Java Developer Onboarding

The backend is split between the Semoss core library and the [Monolith](https://github.com/SEMOSS/Monolith) web application. Semoss builds JAR artifacts; Monolith embeds the core dependency in a WAR deployed on Tomcat.

## Requirements and checkout

Use JDK 21, Maven 3.9.x, and Git. CI currently uses Maven 3.9.9. Docker with Compose v2 is needed for the container examples. Check `mvn -version` as well as `java -version` so Maven uses the intended JDK.

Keep sibling checkouts for the local development scripts:

```bash
git clone https://github.com/SEMOSS/Semoss.git
git clone https://github.com/SEMOSS/Monolith.git
```

```text
workspace/
├── Semoss/
└── Monolith/
```

Import each POM as a Maven project in your IDE. Java sources are in `src/`; Semoss tests are in `test/`, as configured in [pom.xml](../../pom.xml).

## Build order

From the parent directory:

```bash
cd Semoss
mvn clean install
cd ../Monolith
mvn clean package
```

The `ci.version` values must match: Monolith consumes `org.semoss:semoss` with classifier `shaded-dependencies`. `install` makes the core artifact available locally. Monolith produces `target/monolith-<version>.war`.

Use `-DskipTests` when intentionally skipping test execution. The default `dev` profile is the local build path; the `deploy` profile adds release packaging and signing. Semoss's development build also configures the [Git commit hooks](../../hooks/README.md).

## Run locally

For a published-image evaluation, use [Semoss's Compose quick start](../../docker-compose-examples/README.md).

For local backend code, follow [Monolith's Docker quick start](https://github.com/SEMOSS/Monolith#quick-start-with-docker). Its script builds both repositories, creates `local-monolith`, and then invokes Trivy. Run it from `Monolith/local-docker-testing/`:

```bash
bash createLocalDockerScript.sh
cd local-docker-compose
docker compose -f semoss-with-postgres.yml up -d
```

The UI is available at `http://localhost:9090/#/` after startup. Rebuild the image and recreate the application container to pick up changed backend classes.

The development image replaces the WAR and stages JavaScript assets onto a published base. Other Python/R/home assets and frontend changes need their corresponding packaging. It also preserves the base image's `web.xml`; verify the deployed descriptor when testing route or filter changes.

For IDE-managed Tomcat, use Tomcat 11/Jakarta Servlet 6.1 with the matching runtime configuration, SEMOSS home assets, system databases, and frontend. Copying a core JAR or WAR alone does not assemble that environment. See [configuration](configuration_and_environment.md).

## Where to make changes

| Area | Start here |
| --- | --- |
| Pixel parsing and dispatch | [sablecc2](../../src/prerna/sablecc2/), [reactor framework](../concepts/reactor_framework.md) |
| Engine integrations | [engine interfaces and implementations](../../src/prerna/engine/) |
| Agents and native execution | [agent guides](../agents/README.md), [agent source](../../src/prerna/reactor/agent/) |
| Workspace and room persistence | [model inference services](../../src/prerna/engine/impl/model/inferencetracking/) |
| Skill packages and staging | [skill guide](../agents/skills/skills_doc.md), [skill source](../../src/prerna/reactor/agent/skill/) |
| Platform agent defaults | [SystemAgentSeeder](../../src/prerna/util/SystemAgentSeeder.java), [workbench defaults](../agents/workbench-default-agents.md) |
| HTTP routes, filters, and sessions | [Monolith](https://github.com/SEMOSS/Monolith) |
| Frontend workbenches | [semoss-ui](https://github.com/SEMOSS/semoss-ui) |

## Verification

From Semoss:

```bash
mvn test
mvn -Dtest=AgentPromptCompositionUnitTests,SemossAgentPromptUnitTests,SystemAgentSeederUnitTests,RoomMessageStoreUnitTests test
mvn verify
```

The default profile selects `**/*UnitTests.java`. Tests with different names need explicit selection; integrations can also need services and configuration. Surefire reports are under `target/surefire-reports/`; configured JaCoCo output is under `target/site/jacoco/` when generated.

For agent changes, check an ordinary turn, tool execution, approval pause/resume, cancellation, skill loading, and the actual file target. For persistence changes, verify conversation reload and the distinction between durable messages and live stream events. Unit tests do not establish model quality or end-to-end deployment behavior.

Monolith currently has no dedicated test source tree. Use its Maven verification plus affected API/UI flows against the local stack. Keep code, platform project assets, and frontend configuration aligned when changing system agents.

## Contributing

Use focused branches and pull requests with relevant tests, documentation, and validation details. Follow the [commit conventions](../../hooks/README.md). Keep local credentials, absolute machine paths, generated assets, and runtime data out of changes intended for other installations.
