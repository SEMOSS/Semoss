/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockStatic;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.access.EngineGrantee;
import prerna.auth.utils.AbstractSecurityUtilsUnitTestsSetup;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.auth.utils.SecurityUpdateUtils;
import prerna.engine.api.GuardrailTypeEnum;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.guardrail.AbstractGuardrailReactorFunctionEngine;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.AskStringModelEngineResponse;
import prerna.engine.impl.model.usage.ModelUsageCost;
import prerna.engine.impl.pipeline.AgentRunOperation;
import prerna.engine.impl.pipeline.EngineCallPermission;
import prerna.engine.impl.pipeline.EngineProxyFactory;
import prerna.engine.impl.pipeline.StubEngines;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.reactor.IReactor;
import prerna.reactor.interceptor.GenericGuardrailInputReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.GuardrailNounMetadata;

/**
 * One whole chain, as the platform runs it: an app's custom reactor asks a
 * model, the model's pipeline runs a guardrail, and the guardrail asks its
 * judge model. Each test is one rule of docs/platform_services/engine_access.md
 * for calls an engine makes for an app.
 *
 * <p>
 * The pieces are the real ones wherever the access check reads them: the custom
 * reactor is Java source compiled at the start and loaded through the project
 * sandbox as the app's custom code ({@link #REACTOR_SOURCE}); the engines come
 * from {@code Utility.getModel} and {@code Utility.getGuardrailEngine} behind
 * the real proxy; the model's pipeline is a pipeline.json in its assets folder
 * with the real {@link GenericGuardrailInputReactor}; the guardrail is a real
 * engine class ({@link JudgeAskingGuardrail}); and every permission is a row in
 * the junit security database, given and taken away the way the Apps and
 * Engines tables do. Only the models' answers are made up.
 * </p>
 *
 * <p>
 * To debug a hop, set a breakpoint in {@code EngineAccessGuard.check} (or in
 * {@code Caller.fromStack} and {@code Caller.originAppOf}) and run one test in
 * debug. The check stops three times, for the model, the guardrail and the
 * judge, and {@code engineId} says which.
 * </p>
 */
class AppEngineChainUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private static final String APP = "orders-app";
	private static final String OTHER_APP = "other-orders-app";
	private static final String AGENT = "orders-agent";
	private static final String MODEL = "orders-model";
	private static final String GUARDRAIL = "orders-guardrail";
	private static final String JUDGE = "orders-judge";
	private static final String PROJECT_TYPE = "PROJECT";
	private static final String PERSON = "chainperson";

	private static final String REACTOR_CLASS = "orders.app.AskOrdersModelReactor";

	/**
	 * The app's custom reactor, as its author writes it in the app's java folder.
	 */
	private static final String REACTOR_SOURCE = """
			package orders.app;

			import java.util.HashMap;

			import prerna.engine.api.IModelEngine;
			import prerna.engine.impl.model.responses.AskModelEngineResponse;
			import prerna.reactor.AbstractReactor;
			import prerna.sablecc2.om.PixelDataType;
			import prerna.sablecc2.om.nounmeta.NounMetadata;
			import prerna.util.Utility;

			public class AskOrdersModelReactor extends AbstractReactor {

				@Override
				public NounMetadata execute() {
					IModelEngine model = Utility.getModel("%s");
					AskModelEngineResponse<?> answer = model.ask("When does order 42 ship?", null, null, new HashMap<>());
					return new NounMetadata(answer.getResponse(), PixelDataType.CONST_STRING);
				}
			}
			"""
			.formatted(MODEL);

	private static User owner;
	private static User person;
	private static Class<?> reactorClass;

	private final AtomicReference<EngineGrantee> seenByTheModel = new AtomicReference<>();
	private final AtomicReference<EngineGrantee> seenByTheGuardrail = new AtomicReference<>();
	private final AtomicReference<EngineGrantee> seenByTheJudge = new AtomicReference<>();
	private final List<String[]> grants = new ArrayList<>();
	private MockedStatic<ModelUsageCost> prices;

	@BeforeAll
	static void anAppAModelAGuardrailAndAJudgeOwnedBySomeoneElse() throws Exception {
		owner = signedUpUser("chainowner");
		person = signedUpUser(PERSON);
		SecurityProjectUtils.addProject(APP, "Orders App", "CODE", null, false, owner);
		SecurityProjectUtils.addProjectOwner(owner, APP);
		SecurityProjectUtils.addProject(OTHER_APP, "Other Orders App", "CODE", null, false, owner);
		SecurityProjectUtils.addProjectOwner(owner, OTHER_APP);
		SecurityProjectUtils.addProject(AGENT, "Orders Agent", "WORKSPACE", null, false, owner);
		SecurityProjectUtils.addProjectOwner(owner, AGENT);
		SecurityProjectUtils.addProjectUserPermissions(owner, APP,
				List.of(Map.of("userid", PERSON, "permission", "READ_ONLY")),
				ZonedDateTime.now().plusDays(2).toString());
		for (String[] engine : new String[][] { { MODEL, "MODEL" }, { GUARDRAIL, "GUARDRAIL" }, { JUDGE, "MODEL" } }) {
			SecurityEngineUtils.addEngine(engine[0], engine[0], IEngine.CATALOG_TYPE.valueOf(engine[1]), null, null,
					false, owner);
			SecurityEngineUtils.addEngineOwner(owner, engine[0]);
		}
		reactorClass = compiledIntoTheApp(tempDir.resolve("orders-app-classes"));
	}

	@BeforeEach
	void loadTheEnginesAndSignInThePerson() throws Exception {
		prices = mockStatic(ModelUsageCost.class);
		prices.when(() -> ModelUsageCost.hasKnownPrices(anyString())).thenReturn(false);
		prices.when(() -> ModelUsageCost.priceEntry(anyString())).thenReturn(null);

		IModelEngine model = StubEngines.unguardedModel(MODEL, Map.of("ask", () -> {
			seenByTheModel.set(EngineCallPermission.granteeMakingTheCall());
			return new AskStringModelEngineResponse("Order 42 ships on Friday", 0, 0);
		}, "getSmssProp", AppEngineChainUnitTests::settingsWithAPipeline));
		writePipeline(model);
		DIHelperTestUtils.setLoadedEngine(MODEL, model);
		DIHelperTestUtils.setLoadedEngine(GUARDRAIL, new JudgeAskingGuardrail(seenByTheGuardrail));
		DIHelperTestUtils.setLoadedEngine(JUDGE, StubEngines.unguardedModel(JUDGE, Map.of("ask", () -> {
			seenByTheJudge.set(EngineCallPermission.granteeMakingTheCall());
			return new AskStringModelEngineResponse("SAFE", 0, 0);
		})));
		ThreadStore.setUser(person);
	}

	@AfterEach
	void takeTheGrantsAwayAndUnload() throws Exception {
		ThreadStore.remove();
		for (String[] grant : grants) {
			SecurityEngineUtils.removeEngineGranteePermission(owner, grant[0], grant[1], grant[2]);
		}
		grants.clear();
		InternalUtility.removeLoadedEngine(MODEL);
		InternalUtility.removeLoadedEngine(GUARDRAIL);
		InternalUtility.removeLoadedEngine(JUDGE);
		prices.close();
	}

	@Test
	void eachEngineInTheChainGoesThroughOnThePermissionItGivesTheEngineCallingIt() throws Exception {
		give(MODEL, APP, PROJECT_TYPE);
		give(GUARDRAIL, MODEL, "MODEL");
		give(JUDGE, GUARDRAIL, "GUARDRAIL");

		assertEquals("Order 42 ships on Friday", runTheAppsReactor());

		assertEquals(EngineGrantee.ofApp(APP), seenByTheModel.get());
		assertEquals(new EngineGrantee(MODEL, "MODEL"), seenByTheGuardrail.get());
		assertEquals(new EngineGrantee(GUARDRAIL, "GUARDRAIL"), seenByTheJudge.get());
	}

	@Test
	void eachEngineInTheChainGoesThroughOnThePermissionItGivesTheApp() throws Exception {
		give(MODEL, APP, PROJECT_TYPE);
		give(GUARDRAIL, APP, PROJECT_TYPE);
		give(JUDGE, APP, PROJECT_TYPE);

		assertEquals("Order 42 ships on Friday", runTheAppsReactor());

		assertEquals(EngineGrantee.ofApp(APP), seenByTheModel.get());
		assertEquals(EngineGrantee.ofApp(APP), seenByTheGuardrail.get());
		assertEquals(EngineGrantee.ofApp(APP), seenByTheJudge.get());
	}

	@Test
	void anEngineThatGivesNeitherTheEngineCallingItNorTheAppAPermissionStopsTheChain() throws Exception {
		give(MODEL, APP, PROJECT_TYPE);
		give(GUARDRAIL, MODEL, "MODEL");

		SemossPixelException refusal = assertThrows(SemossPixelException.class, this::runTheAppsReactor);

		assertTrue(refusal.getMessage().startsWith("Engine " + JUDGE + " does not exist"), refusal.getMessage());
		assertTrue(refusal.getMessage().contains("engine " + GUARDRAIL), refusal.getMessage());
		assertTrue(refusal.getMessage().contains("app " + APP), refusal.getMessage());
		assertNull(seenByTheModel.get());
	}

	@Test
	void anAppTheModelGivesNothingIsRefusedAtTheModel() {
		give(GUARDRAIL, APP, PROJECT_TYPE);
		give(JUDGE, APP, PROJECT_TYPE);

		SemossPixelException refusal = assertThrows(SemossPixelException.class, this::runTheAppsReactor);

		assertTrue(refusal.getMessage().startsWith("Engine " + MODEL + " does not exist"), refusal.getMessage());
		assertNull(seenByTheGuardrail.get());
	}

	@Test
	void takingTheAppsPermissionAwayOnTheModelStopsTheChainAtTheModel() throws Exception {
		give(MODEL, APP, PROJECT_TYPE);
		give(GUARDRAIL, APP, PROJECT_TYPE);
		give(JUDGE, APP, PROJECT_TYPE);
		assertEquals("Order 42 ships on Friday", runTheAppsReactor());

		takeAway(MODEL, APP, PROJECT_TYPE);

		SemossPixelException refusal = assertThrows(SemossPixelException.class, this::runTheAppsReactor);
		assertTrue(refusal.getMessage().startsWith("Engine " + MODEL + " does not exist"), refusal.getMessage());
	}

	@Test
	void aPermissionTheModelGivesAnotherAppDoesNotCount() {
		give(MODEL, OTHER_APP, PROJECT_TYPE);
		give(GUARDRAIL, APP, PROJECT_TYPE);
		give(JUDGE, APP, PROJECT_TYPE);

		SemossPixelException refusal = assertThrows(SemossPixelException.class, this::runTheAppsReactor);

		assertTrue(refusal.getMessage().startsWith("Engine " + MODEL + " does not exist"), refusal.getMessage());
		assertNull(seenByTheModel.get());
	}

	@Test
	void anAgentRunAroundTheAppsReactorLendsTheReactorNothing() {
		give(MODEL, AGENT, PROJECT_TYPE);
		give(GUARDRAIL, APP, PROJECT_TYPE);
		give(JUDGE, APP, PROJECT_TYPE);

		try (AgentRunOperation run = AgentRunOperation.begin(AGENT)) {
			SemossPixelException refusal = assertThrows(SemossPixelException.class, this::runTheAppsReactor);
			assertTrue(refusal.getMessage().startsWith("Engine " + MODEL + " does not exist"), refusal.getMessage());
		}
		assertNull(seenByTheModel.get());
	}

	/**
	 * Runs the app's custom reactor for the person signed in on the thread, as a
	 * pixel that calls it does.
	 *
	 * @return what the reactor returned
	 * @throws Exception when the reactor cannot be made
	 */
	private Object runTheAppsReactor() throws Exception {
		IReactor reactor = (IReactor) reactorClass.getDeclaredConstructor().newInstance();
		return reactor.execute().getValue();
	}

	/**
	 * Gives an app or an engine Read-Only on an engine, as its owner does from the
	 * engine's Apps or Engines table, and takes it away after the test.
	 *
	 * @param engineId    the engine giving the permission
	 * @param granteeId   the app or engine given it
	 * @param granteeType its catalog type, PROJECT for an app
	 */
	private void give(String engineId, String granteeId, String granteeType) {
		try {
			SecurityEngineUtils.setEngineGranteePermission(owner, granteeId, granteeType, engineId, "READ_ONLY", null);
		} catch (IllegalAccessException e) {
			throw new IllegalStateException(e);
		}
		grants.add(new String[] { granteeId, granteeType, engineId });
	}

	/**
	 * Takes an app's or an engine's permission on an engine away during the test,
	 * as its owner does from the engine's Apps or Engines table.
	 *
	 * @param engineId    the engine that gave the permission
	 * @param granteeId   the app or engine given it
	 * @param granteeType its catalog type, PROJECT for an app
	 * @throws IllegalAccessException when the owner may not take it away
	 */
	private void takeAway(String engineId, String granteeId, String granteeType) throws IllegalAccessException {
		SecurityEngineUtils.removeEngineGranteePermission(owner, granteeId, granteeType, engineId);
		grants.removeIf(
				grant -> grant[0].equals(granteeId) && grant[1].equals(granteeType) && grant[2].equals(engineId));
	}

	private static Properties settingsWithAPipeline() {
		Properties smss = new Properties();
		smss.setProperty(IEngine.PIPELINE, "pipeline.json");
		return smss;
	}

	/**
	 * Writes the model's pipeline.json, which runs the guardrail on the question of
	 * every {@code ask}, as Settings > Guardrails saves it.
	 *
	 * @param model the model, unproxied
	 * @throws IOException           when the file cannot be written
	 * @throws NoSuchMethodException when the model has no {@code ask}
	 */
	private static void writePipeline(IModelEngine model) throws IOException, NoSuchMethodException {
		String question = IModelEngine.class.getMethod("ask", String.class, String.class, Insight.class, Map.class)
				.getParameters()[0].getName();
		File pipeline = EngineProxyFactory.pipelineFile(model, "pipeline.json");
		Files.createDirectories(pipeline.toPath().getParent());
		Files.writeString(pipeline.toPath(), """
				{"pipelines": {"ask": {"input": [{
					"reactorClass": "%s",
					"params": {"checkId": "check0", "guardrailEngineId": "%s", "inputMapping": {"prompt": "%s"}}
				}]}}}
				""".formatted(GenericGuardrailInputReactor.class.getName(), GUARDRAIL, question),
				StandardCharsets.UTF_8);
	}

	private static User signedUpUser(String id) {
		AccessToken token = new AccessToken();
		token.setId(id);
		token.setName(id);
		token.setUsername(id);
		token.setEmail(id + "@test.com");
		token.setProvider(AuthProvider.NATIVE);
		assertTrue(SecurityUpdateUtils.registerUser(id, id, id + "@test.com", "Test123!",
				AuthProvider.NATIVE.getLabel(), "5555555555", "001", "US", false, false, false, null));
		User user = new User();
		user.setPrimaryLogin(AuthProvider.NATIVE);
		user.setAccessToken(token);
		return user;
	}

	/**
	 * Compiles {@link #REACTOR_SOURCE} into a classes folder and loads it through
	 * the project sandbox, as the app's custom code.
	 *
	 * @param classes the app's classes folder
	 * @return the reactor's class, loaded as the app's
	 * @throws Exception when it does not compile or load
	 */
	private static Class<?> compiledIntoTheApp(Path classes) throws Exception {
		Path source = classes.resolveSibling("orders-app-java").resolve(REACTOR_CLASS.replace('.', '/') + ".java");
		Files.createDirectories(source.getParent());
		Files.createDirectories(classes);
		Files.writeString(source, REACTOR_SOURCE, StandardCharsets.UTF_8);
		JavaCompiler javac = ToolProvider.getSystemJavaCompiler();
		ByteArrayOutputStream errors = new ByteArrayOutputStream();
		int status = javac.run(null, null, errors, "--release", "21", "-g", "-proc:none", "-nowarn", "-classpath",
				testClassPath(), "-d", classes.toString(), source.toString());
		assertEquals(0, status, errors.toString(StandardCharsets.UTF_8));
		return SandboxProbe.loader(classes, APP).loadClass(REACTOR_CLASS);
	}

	/**
	 * The classpath the tests run with. Maven runs them from one jar whose manifest
	 * lists the classpath, so that jar's list is read.
	 *
	 * @return the classpath, for javac
	 * @throws IOException when the jar cannot be read
	 */
	private static String testClassPath() throws IOException {
		String classPath = System.getProperty("java.class.path");
		String[] entries = classPath.split(File.pathSeparator);
		if (entries.length != 1 || !entries[0].endsWith(".jar")) {
			return classPath;
		}
		try (JarFile jar = new JarFile(entries[0])) {
			Manifest manifest = jar.getManifest();
			String listed = manifest == null ? null : manifest.getMainAttributes().getValue(Attributes.Name.CLASS_PATH);
			if (listed == null || listed.isBlank()) {
				return classPath;
			}
			URI base = Path.of(entries[0]).toUri();
			List<String> paths = new ArrayList<>();
			for (String entry : listed.trim().split("\\s+")) {
				paths.add(Path.of(base.resolve(entry)).toString());
			}
			return String.join(File.pathSeparator, paths);
		}
	}

	/**
	 * A guardrail that asks its judge model whether a prompt is safe, as the prompt
	 * guardrails do: its own code gets the judge from {@code Utility.getModel} and
	 * asks it, so the judge's access check sees the guardrail acting.
	 */
	static final class JudgeAskingGuardrail extends AbstractGuardrailReactorFunctionEngine {

		private final AtomicReference<EngineGrantee> seen;

		JudgeAskingGuardrail(AtomicReference<EngineGrantee> seen) {
			this.seen = seen;
			setEngineId(GUARDRAIL);
			setEngineName(GUARDRAIL);
			setSmssProp(new Properties());
		}

		@Override
		public GuardrailNounMetadata execute(NounStore ns, GenRowStruct curRow) {
			this.seen.set(EngineCallPermission.granteeMakingTheCall());
			GenRowStruct prompt = ns.getGenRowStruct("prompt");
			String text = prompt == null || prompt.isEmpty() ? "" : String.valueOf(prompt.get(0));
			AskModelEngineResponse<?> verdict = Utility.getModel(JUDGE).ask("Is this safe? " + text, null, null,
					new HashMap<>());
			boolean pass = "SAFE".equals(verdict.getResponse());
			return new GuardrailNounMetadata(pass, text, Map.of("classification", verdict.getResponse()));
		}

		@Override
		public GuardrailTypeEnum getGuardrailType() {
			return GuardrailTypeEnum.EMBEDDED_AGGRESSIVE_SELF_HARM;
		}

	}

}
