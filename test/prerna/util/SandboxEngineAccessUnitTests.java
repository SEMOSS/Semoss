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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;

import java.io.Closeable;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.objectweb.asm.Handle;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineAccessUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.api.IExplorable;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.pipeline.AdminEngineCalls;
import prerna.engine.impl.pipeline.EngineCallAppPermission;
import prerna.engine.impl.pipeline.PlatformOperation;
import prerna.engine.impl.pipeline.StubEngines;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.om.ThreadStore;
import prerna.project.api.IProject;
import prerna.sablecc2.om.execptions.SemossPixelException;

/**
 * The access check the engine proxy runs on every call, for calls a project's
 * custom code makes. Each test loads a class through the project sandbox, as a
 * custom reactor is loaded, and has it call a method of an engine or project
 * behind the real proxy. Each test is one rule of
 * docs/platform_services/engine_access.md. The calls the platform's own code
 * makes are in prerna.engine.impl.pipeline.EngineAccessGuardUnitTests.
 */
class SandboxEngineAccessUnitTests {

	private static final String ENGINE = "engine-1";
	private static final String APP = "app-1";
	private static final String OTHER_APP = "app-2";

	private final User user = new User();
	private MockedStatic<SecurityEngineAccessUtils> access;

	@TempDir
	Path folder;

	@BeforeEach
	void aServerWithOneSignedInUserWhoHoldsNothing() {
		this.access = Mockito.mockStatic(SecurityEngineAccessUtils.class);
		this.access.when(SecurityEngineAccessUtils::isSecurityDatabaseLoaded).thenReturn(true);
		this.access.when(() -> SecurityEngineAccessUtils.isListed(anyString(), anyBoolean())).thenReturn(true);
		ThreadStore.setUser(this.user);
		SandboxProbe.heldObject = StubEngines.database(ENGINE);
	}

	@AfterEach
	void clear() {
		this.access.close();
		ThreadStore.remove();
		SandboxProbe.heldObject = null;
	}

	@Test
	void aCustomReactorReadsAnEngineTheUserCanView() throws Throwable {
		userHolds(ENGINE, false, AccessPermissionEnum.READ_ONLY);

		assertEquals(ENGINE, callAs(APP, IEngine.class, "getEngineId"));
	}

	@Test
	void aCustomReactorIsRefusedAnEngineTheUserCannotView() {
		SemossPixelException refusal = assertThrows(SemossPixelException.class,
				() -> callAs(APP, IEngine.class, "getEngineId"));

		assertEquals("Engine " + ENGINE + " does not exist or neither the user nor the app has READ_ONLY permission "
				+ "on it, which a custom reactor needs to call getEngineId", refusal.getMessage());
	}

	@ParameterizedTest
	@ValueSource(strings = { "getConnection", "getPreparedStatement", "execQuery", "getQueryUtil",
			"getConnectionMetadata", "executeInTransaction", "getOWLEngineFactory", "getBaseDataEngine", "commitOWL",
			"insertData", "removeData", "commit", "setAutoCommit" })
	void aCustomReactorNeedsEditToWriteOrToHoldAConnection(String method) throws Throwable {
		userHolds(ENGINE, false, AccessPermissionEnum.READ_ONLY);
		assertThrows(SemossPixelException.class, () -> callAs(APP, IRDBMSEngine.class, method));

		userHolds(ENGINE, false, AccessPermissionEnum.EDIT);
		assertDoesNotThrow(() -> callAs(APP, IRDBMSEngine.class, method));
	}

	@ParameterizedTest
	@ValueSource(strings = { "getSmssProp", "getOrigSmssProp", "getSmssFilePath", "getProperty", "addProperty",
			"getConnectionUrl", "getDataSource", "closeDataSource", "setSmssProp", "open", "close", "delete",
			"setEngineId", "setQueryUtil", "setConnection" })
	void aCustomReactorNeedsOwnerForAnEnginesSettingsAndItsLifecycle(String method) throws Throwable {
		userHolds(ENGINE, false, AccessPermissionEnum.EDIT);
		assertThrows(SemossPixelException.class, () -> callAs(APP, IRDBMSEngine.class, method));

		userHolds(ENGINE, false, AccessPermissionEnum.OWNER);
		assertDoesNotThrow(() -> callAs(APP, IRDBMSEngine.class, method));
	}

	@Test
	void aRefusalTellsACustomReactorWhichLevelTheCallNeeds() {
		userHolds(ENGINE, false, AccessPermissionEnum.READ_ONLY);

		SemossPixelException refusal = assertThrows(SemossPixelException.class,
				() -> callAs(APP, IRDBMSEngine.class, "getConnection"));

		assertEquals("Engine " + ENGINE + " does not exist or neither the user nor the app has EDIT permission on "
				+ "it, which a custom reactor needs to call getConnection", refusal.getMessage());
	}

	@Test
	void anAppsPermissionStandsInForTheUsers() throws Throwable {
		appHolds(APP, AccessPermissionEnum.EDIT);

		assertEquals(ENGINE, callAs(APP, IEngine.class, "getEngineId"));
		assertDoesNotThrow(() -> callAs(APP, IRDBMSEngine.class, "getConnection"));
		assertDoesNotThrow(() -> callAs(APP, IRDBMSEngine.class, "insertData"));
		assertDoesNotThrow(() -> callAs(APP, IRDBMSEngine.class, "getPreparedStatement"));
	}

	@Test
	void aReadOnlyAppPermissionDoesNotGiveAConnection() throws Throwable {
		appHolds(APP, AccessPermissionEnum.READ_ONLY);

		assertEquals(ENGINE, callAs(APP, IEngine.class, "getEngineId"));
		assertThrows(SemossPixelException.class, () -> callAs(APP, IRDBMSEngine.class, "getConnection"));
		assertThrows(SemossPixelException.class, () -> callAs(APP, IRDBMSEngine.class, "insertData"));
	}

	@ParameterizedTest
	@ValueSource(strings = { "getSmssProp", "getOrigSmssProp", "getProperty", "getConnectionUrl", "getDataSource",
			"delete", "close" })
	void anAppsPermissionNeverReachesAnEnginesSettings(String method) {
		appHolds(APP, AccessPermissionEnum.EDIT);

		assertThrows(SemossPixelException.class, () -> callAs(APP, IRDBMSEngine.class, method));
	}

	@Test
	void anotherAppsPermissionDoesNotCount() {
		appHolds(OTHER_APP, AccessPermissionEnum.EDIT);

		assertThrows(SemossPixelException.class, () -> callAs(APP, IEngine.class, "getEngineId"));
	}

	@Test
	void customCodeOfNoProjectHasNoAppPermission() {
		appHolds(APP, AccessPermissionEnum.EDIT);

		assertThrows(SemossPixelException.class, () -> callAs(null, IEngine.class, "getEngineId"));
	}

	@Test
	void beingAnAdminGivesACustomReactorNothing() {
		this.access.when(() -> SecurityEngineAccessUtils.isAdmin(this.user)).thenReturn(true);

		assertThrows(SemossPixelException.class, () -> callAs(APP, IEngine.class, "getEngineId"));
		assertDoesNotThrow(() -> AdminEngineCalls.run(((IEngine) SandboxProbe.heldObject)::getEngineId));
	}

	@Test
	void aCustomReactorWithNoSignedInUserIsRefused() {
		ThreadStore.remove();
		appHolds(APP, AccessPermissionEnum.READ_ONLY);

		SemossPixelException refusal = assertThrows(SemossPixelException.class,
				() -> callAs(APP, IEngine.class, "getEngineId"));

		assertEquals("A custom reactor needs a signed in user to call getEngineId on Engine " + ENGINE,
				refusal.getMessage());
	}

	@Test
	void aCustomReactorIsNotCheckedAsTheUserOfTheInsightTheThreadNames() {
		ThreadStore.remove();
		Insight insight = new Insight();
		insight.setUser(this.user);
		String insightId = InsightStore.getInstance().put(insight);
		ThreadStore.setInsightId(insightId);
		userHolds(ENGINE, false, AccessPermissionEnum.EDIT);
		try {
			assertThrows(SemossPixelException.class, () -> callAs(APP, IEngine.class, "getEngineId"));
		} finally {
			InsightStore.getInstance().remove(insightId);
		}
	}

	@Test
	void aDiscoverableEngineIsNotReadableByACustomReactor() {
		this.access.when(() -> SecurityEngineAccessUtils.isReadableThroughPlatform(this.user, ENGINE, false))
				.thenReturn(true);

		assertThrows(SemossPixelException.class, () -> callAs(APP, IEngine.class, "getEngineId"));
	}

	@Test
	void anEngineTheSecurityDatabaseDoesNotListIsRefusedToACustomReactor() {
		this.access.when(() -> SecurityEngineAccessUtils.isListed(ENGINE, false)).thenReturn(false);

		assertThrows(SemossPixelException.class, () -> callAs(APP, IEngine.class, "getEngineId"));
	}

	@Test
	void aCallThroughALambdaAndTheJdkIsStillTheCustomReactors() throws Throwable {
		userHolds(ENGINE, false, AccessPermissionEnum.READ_ONLY);
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("run", mv -> {
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/util/Optional", "empty", "()Ljava/util/Optional;", false);
			settingsSupplier(mv);
			mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/util/Optional", "orElseGet",
					"(Ljava/util/function/Supplier;)Ljava/lang/Object;", false);
		}).bytes(), APP);

		assertThrows(SemossPixelException.class, () -> SandboxProbe.run(probe, "run"));

		userHolds(ENGINE, false, AccessPermissionEnum.OWNER);
		assertEquals(new Properties(), SandboxProbe.run(probe, "run"));
	}

	@Test
	void aLambdaACustomReactorHandsToThePlatformIsStillTheCustomReactors() throws Throwable {
		userHolds(ENGINE, false, AccessPermissionEnum.READ_ONLY);
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("run", mv -> {
			settingsSupplier(mv);
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, "prerna/util/SandboxProbe", "runAsPlatform",
					"(Ljava/util/function/Supplier;)Ljava/lang/Object;", false);
		}).bytes(), APP);

		assertThrows(SemossPixelException.class, () -> SandboxProbe.run(probe, "run"));
		assertDoesNotThrow(
				() -> SandboxProbe.runAsPlatform(((IEngine) SandboxProbe.heldObject)::getSmssProp));
	}

	@Test
	void aPlatformOperationDoesNotCoverACallACustomReactorMakes() {
		try (PlatformOperation operation = PlatformOperation.begin("cluster sync")) {
			assertThrows(SemossPixelException.class, () -> callAs(APP, IEngine.class, "getEngineId"));
		}
	}

	@Test
	void aPlatformOperationNeverCoversPlatformCodeACustomReactorRuns() throws Throwable {
		Class<?> probe = SandboxProbe.load(this.folder,
				new SandboxProbe().method("run", mv -> mv.visitMethodInsn(Opcodes.INVOKESTATIC,
						"prerna/util/SandboxProbe", "readHeldEngineInAPlatformOperation", "()Ljava/lang/Object;", false))
						.bytes(),
				APP);

		assertThrows(SemossPixelException.class, () -> SandboxProbe.run(probe, "run"));
		try (PlatformOperation operation = PlatformOperation.begin("cluster sync")) {
			assertThrows(SemossPixelException.class, () -> SandboxProbe.run(probe, "run"));
		}
		ThreadStore.remove();
		assertThrows(SemossPixelException.class, () -> SandboxProbe.run(probe, "run"));
		assertDoesNotThrow(SandboxProbe::readHeldEngineInAPlatformOperation);
	}

	@Test
	void anAdminOperationNeverRunsUnderACustomReactor() throws Throwable {
		this.access.when(() -> SecurityEngineAccessUtils.isAdmin(this.user)).thenReturn(true);
		Class<?> probe = SandboxProbe.load(this.folder,
				new SandboxProbe().method("run", mv -> mv.visitMethodInsn(Opcodes.INVOKESTATIC,
						"prerna/util/SandboxProbe", "readHeldEngineThroughAnAdminClass", "()Ljava/lang/Object;", false))
						.bytes(),
				APP);

		assertThrows(SemossPixelException.class, () -> SandboxProbe.run(probe, "run"));
		assertEquals("Stub " + ENGINE, SandboxProbe.readHeldEngineThroughAnAdminClass());
	}

	@Test
	void aCustomReactorHasEditOnItsOwnProject() throws Throwable {
		SandboxProbe.heldObject = StubEngines.project(APP);

		assertEquals(APP, callAs(APP, IProject.class, "getProjectId"));
		assertDoesNotThrow(() -> callAs(APP, IProject.class, "getProjectProperties"));
		assertDoesNotThrow(() -> callAs(APP, IProject.class, "getInsightDatabase"));
		assertDoesNotThrow(() -> callAs(APP, IProject.class, "compileReactors"));
		assertThrows(SemossPixelException.class, () -> callAs(APP, IProject.class, "delete"));
		assertThrows(SemossPixelException.class, () -> callAs(APP, IProject.class, "getSmssProp"));
	}

	@Test
	void aCustomReactorNeedsTheUsersPermissionOnAnotherProject() throws Throwable {
		SandboxProbe.heldObject = StubEngines.project(OTHER_APP);
		assertThrows(SemossPixelException.class, () -> callAs(APP, IProject.class, "getProjectId"));

		userHolds(OTHER_APP, true, AccessPermissionEnum.READ_ONLY);
		assertEquals(OTHER_APP, callAs(APP, IProject.class, "getProjectId"));
		assertThrows(SemossPixelException.class, () -> callAs(APP, IProject.class, "getProjectProperties"));
	}

	@Test
	void aModelAnsweringACallMadeWithAnAppsPermissionKnowsTheApp() throws Throwable {
		AtomicReference<String> appSeenByTheModel = new AtomicReference<>("not called");
		AtomicReference<String> appSeenByAnEngineTheModelCalls = new AtomicReference<>("not called");
		IRDBMSEngine configured = StubEngines.database("configured-on-the-model", Map.of("getSchema", () -> {
			appSeenByAnEngineTheModelCalls.set(EngineCallAppPermission.appMakingTheCall());
			return null;
		}));
		SandboxProbe.heldObject = StubEngines.model(ENGINE, Map.of("getContextWindow", () -> {
			appSeenByTheModel.set(EngineCallAppPermission.appMakingTheCall());
			configured.getSchema();
			return 0;
		}));
		appHolds(APP, AccessPermissionEnum.READ_ONLY);

		callAs(APP, IModelEngine.class, "getContextWindow");

		assertEquals(APP, appSeenByTheModel.get());
		assertNull(appSeenByAnEngineTheModelCalls.get());
		assertNull(EngineCallAppPermission.appMakingTheCall());
	}

	@Test
	void aModelKnowsTheAppMakingTheCallWhenTheUserHasAccessOfTheirOwn() throws Throwable {
		AtomicReference<String> appSeenByTheModel = new AtomicReference<>("not called");
		SandboxProbe.heldObject = StubEngines.model(ENGINE, Map.of("getContextWindow", () -> {
			appSeenByTheModel.set(EngineCallAppPermission.appMakingTheCall());
			return 0;
		}));
		appHolds(APP, AccessPermissionEnum.READ_ONLY);
		userHolds(ENGINE, false, AccessPermissionEnum.READ_ONLY);

		callAs(APP, IModelEngine.class, "getContextWindow");

		assertEquals(APP, appSeenByTheModel.get());
		assertNull(EngineCallAppPermission.appMakingTheCall());
	}

	@Test
	void aModelSeesNoAppWhenItGivesTheCallingAppNoPermission() throws Throwable {
		AtomicReference<String> appSeenByTheModel = new AtomicReference<>("not called");
		SandboxProbe.heldObject = StubEngines.model(ENGINE, Map.of("getContextWindow", () -> {
			appSeenByTheModel.set(EngineCallAppPermission.appMakingTheCall());
			return 0;
		}));
		appHolds(OTHER_APP, AccessPermissionEnum.READ_ONLY);
		userHolds(ENGINE, false, AccessPermissionEnum.READ_ONLY);

		callAs(APP, IModelEngine.class, "getContextWindow");

		assertNull(appSeenByTheModel.get());
	}

	@Test
	void aModelSeesNoAppOnACallThePlatformMakes() {
		AtomicReference<String> appSeenByTheModel = new AtomicReference<>("not called");
		IModelEngine model = StubEngines.model(ENGINE, Map.of("getContextWindow", () -> {
			appSeenByTheModel.set(EngineCallAppPermission.appMakingTheCall());
			return 0;
		}));
		appHolds(APP, AccessPermissionEnum.READ_ONLY);
		userHolds(ENGINE, false, AccessPermissionEnum.READ_ONLY);

		model.getContextWindow();

		assertNull(appSeenByTheModel.get());
	}

	@Test
	void aRefusedCallLeavesNoAppBehind() {
		SandboxProbe.heldObject = StubEngines.model(ENGINE);

		assertThrows(SemossPixelException.class, () -> callAs(APP, IModelEngine.class, "getContextWindow"));

		assertNull(EngineCallAppPermission.appMakingTheCall());
	}

	@Test
	void aProjectsLoaderNamesItsProject() throws Exception {
		Class<?> ofAnApp = SandboxProbe.load(this.folder.resolve("app"),
				new SandboxProbe().method("run", mv -> mv.visitInsn(Opcodes.ACONST_NULL)).bytes(), APP);
		Class<?> ofNoProject = SandboxProbe.load(this.folder.resolve("none"),
				new SandboxProbe().method("run", mv -> mv.visitInsn(Opcodes.ACONST_NULL)).bytes());

		assertEquals(APP, SandboxedJavaExecution.projectIdOf(ofAnApp));
		assertNull(SandboxedJavaExecution.projectIdOf(ofNoProject));
		assertNull(SandboxedJavaExecution.projectIdOf(SandboxEngineAccessUnitTests.class));
		assertNull(SandboxedJavaExecution.projectIdOf(null));
	}

	private void userHolds(String id, boolean project, AccessPermissionEnum level) {
		this.access.when(() -> SecurityEngineAccessUtils.getUserPermission(this.user, id, project)).thenReturn(level);
	}

	private void appHolds(String projectId, AccessPermissionEnum level) {
		this.access.when(() -> SecurityEngineAccessUtils.givesAProjectPermission(ENGINE)).thenReturn(true);
		this.access.when(() -> SecurityEngineAccessUtils.getProjectPermission(projectId, ENGINE)).thenReturn(level);
	}

	/**
	 * Has the custom code of a project call one method of the engine or project the
	 * test holds, with null and zero arguments.
	 *
	 * @param projectId the project the calling class belongs to, or null for none
	 * @param type      the interface the call names
	 * @param name      the method; its first overload is called
	 * @return what the method returned
	 * @throws Throwable what the call threw
	 */
	private Object callAs(String projectId, Class<?> type, String name) throws Throwable {
		Method method = method(type, name);
		Class<?> declaredBy = method.getDeclaringClass() == Closeable.class ? IEngine.class
				: method.getDeclaringClass();
		String owner = Type.getInternalName(declaredBy);
		String descriptor = Type.getMethodDescriptor(method);
		Consumer<MethodVisitor> body = mv -> {
			mv.visitFieldInsn(Opcodes.GETSTATIC, "prerna/util/SandboxProbe", "heldObject", "Ljava/lang/Object;");
			mv.visitTypeInsn(Opcodes.CHECKCAST, owner);
			for (Type argument : Type.getArgumentTypes(descriptor)) {
				switch (argument.getSort()) {
				case Type.OBJECT, Type.ARRAY -> mv.visitInsn(Opcodes.ACONST_NULL);
				case Type.LONG -> mv.visitInsn(Opcodes.LCONST_0);
				case Type.FLOAT -> mv.visitInsn(Opcodes.FCONST_0);
				case Type.DOUBLE -> mv.visitInsn(Opcodes.DCONST_0);
				default -> mv.visitInsn(Opcodes.ICONST_0);
				}
			}
			mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, owner, name, descriptor, true);
			switch (Type.getReturnType(descriptor).getSort()) {
			case Type.OBJECT, Type.ARRAY -> {
			}
			case Type.VOID -> mv.visitInsn(Opcodes.ACONST_NULL);
			case Type.LONG, Type.DOUBLE -> {
				mv.visitInsn(Opcodes.POP2);
				mv.visitInsn(Opcodes.ACONST_NULL);
			}
			default -> {
				mv.visitInsn(Opcodes.POP);
				mv.visitInsn(Opcodes.ACONST_NULL);
			}
			}
		};
		Path classes = this.folder.resolve(name + "-" + projectId);
		Class<?> probe = SandboxProbe.load(classes, new SandboxProbe().method("run", body).bytes(), projectId);
		return SandboxProbe.run(probe, "run");
	}

	private static Method method(Class<?> type, String name) {
		for (Class<?> candidate : new Class<?>[] { type, IRDBMSEngine.class, IDatabaseEngine.class,
				IExplorable.class, IModelEngine.class, IEngine.class }) {
			for (Method method : candidate.getMethods()) {
				if (method.getName().equals(name)) {
					return method;
				}
			}
		}
		throw new AssertionError("No method named " + name);
	}

	/**
	 * Leaves on the stack a supplier of the held engine's settings, written as the
	 * method reference {@code engine::getSmssProp}.
	 *
	 * @param mv the method being written
	 */
	private static void settingsSupplier(MethodVisitor mv) {
		String engine = Type.getInternalName(IEngine.class);
		mv.visitFieldInsn(Opcodes.GETSTATIC, "prerna/util/SandboxProbe", "heldObject", "Ljava/lang/Object;");
		mv.visitTypeInsn(Opcodes.CHECKCAST, engine);
		mv.visitInvokeDynamicInsn("get", "(L" + engine + ";)Ljava/util/function/Supplier;",
				new Handle(Opcodes.H_INVOKESTATIC, "java/lang/invoke/LambdaMetafactory", "metafactory",
						"(Ljava/lang/invoke/MethodHandles$Lookup;Ljava/lang/String;Ljava/lang/invoke/MethodType;"
								+ "Ljava/lang/invoke/MethodType;Ljava/lang/invoke/MethodHandle;"
								+ "Ljava/lang/invoke/MethodType;)Ljava/lang/invoke/CallSite;",
						false),
				Type.getType("()Ljava/lang/Object;"),
				new Handle(Opcodes.H_INVOKEINTERFACE, engine, "getSmssProp", "()Ljava/util/Properties;", true),
				Type.getType("()Ljava/util/Properties;"));
	}

}
