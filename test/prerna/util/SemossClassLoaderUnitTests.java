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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Handle;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

class SemossClassLoaderUnitTests {

	private static final String PROBE = "custom/sandbox/Probe";
	private static final Handle LAMBDA_METAFACTORY = new Handle(Opcodes.H_INVOKESTATIC,
			"java/lang/invoke/LambdaMetafactory", "metafactory",
			"(Ljava/lang/invoke/MethodHandles$Lookup;Ljava/lang/String;Ljava/lang/invoke/MethodType;"
					+ "Ljava/lang/invoke/MethodType;Ljava/lang/invoke/MethodHandle;Ljava/lang/invoke/MethodType;)"
					+ "Ljava/lang/invoke/CallSite;",
			false);

	@TempDir
	Path folder;

	private final List<URLClassLoader> jarLoaders = new ArrayList<>();
	private Properties savedCoreProp;

	@BeforeEach
	void setRdfMap() {
		this.savedCoreProp = DIHelper.getInstance().getCoreProp();
		Properties rdfMap = new Properties();
		rdfMap.setProperty(Constants.BASE_FOLDER, "/opt/semosshome");
		rdfMap.setProperty("SMTP_PASSWORD", "smtp-secret-value");
		DIHelper.getInstance().setCoreProp(rdfMap);
	}

	@AfterEach
	void restoreRdfMap() throws Exception {
		SandboxedJavaExecution.clearSandboxRootForCurrentThread();
		DIHelper.getInstance().setCoreProp(this.savedCoreProp);
		for (URLClassLoader loader : this.jarLoaders) {
			loader.close();
		}
	}

	@ParameterizedTest
	@ValueSource(strings = { "prerna.util.DIHelper", "prerna.util.DIHelper$Holder", "prerna.util.SystemEngineRegistry",
			"prerna.security.SecretRedactor", "prerna.util.InternalUtility" })
	void platformClassesThatHoldEnginesOrSecretsAreBlocked(String name) {
		assertTrue(SemossClassLoader.isReflectionApiBlocked(name));
	}

	@ParameterizedTest
	@ValueSource(strings = { "prerna.util.Utility", "prerna.util.DIHelperView", "prerna.security.SensitiveKeys",
			"java.lang.String" })
	void otherClassesAreNotBlockedByName(String name) {
		assertFalse(SemossClassLoader.isReflectionApiBlocked(name));
	}

	@Test
	void aChildLoaderAskingTheSandboxForABlockedClassIsRefused() throws Exception {
		try (URLClassLoader child = new URLClassLoader(new URL[0], new SemossClassLoader(testLoader()))) {
			assertThrows(ClassNotFoundException.class, () -> child.loadClass("prerna.util.DIHelper"));
			assertThrows(ClassNotFoundException.class, () -> child.loadClass("java.lang.reflect.Field"));
			assertEquals(Utility.class, child.loadClass("prerna.util.Utility"));
		}
	}

	@Test
	void folderReactorsCannotUseDIHelper() throws Exception {
		Class<?> probe = loadFromFolder();

		InvocationTargetException failure = assertThrows(InvocationTargetException.class,
				() -> call(probe, "touchDIHelper"));
		assertInstanceOf(NoClassDefFoundError.class, failure.getCause());
	}

	@Test
	void folderReactorsCannotLoadTheRedactorByName() throws Exception {
		Class<?> probe = loadFromFolder();

		InvocationTargetException failure = assertThrows(InvocationTargetException.class,
				() -> call(probe, "findRedactor"));
		assertInstanceOf(SecurityException.class, failure.getCause());
	}

	@Test
	void folderReactorsReadSettingsButNotSecretSettings() throws Exception {
		Class<?> probe = loadFromFolder();

		assertEquals("/opt/semosshome", call(probe, "readSetting"));
		assertNull(call(probe, "readSecretSetting"));
	}

	@Test
	void folderReactorsCannotUseInternalUtility() throws Exception {
		Class<?> probe = loadFromFolder();

		InvocationTargetException failure = assertThrows(InvocationTargetException.class,
				() -> call(probe, "touchInternalUtility"));
		assertInstanceOf(NoClassDefFoundError.class, failure.getCause());
	}

	@Test
	void folderReactorsCannotReadTheDIHelperLocalMap() throws Exception {
		Class<?> probe = loadFromFolder();

		InvocationTargetException failure = assertThrows(InvocationTargetException.class,
				() -> call(probe, "readLocalProperty"));
		assertInstanceOf(SecurityException.class, failure.getCause());
		assertTrue(failure.getCause().getMessage().contains("Utility.getDIHelperLocalProperty"));
	}

	@Test
	void withoutAChrootLoadPropertiesReadsTheFileAsBefore() throws Exception {
		Class<?> probe = loadFromFolder();

		Properties loaded = (Properties) call(probe, "loadSmss");

		assertEquals("sk-live-engine-key", loaded.getProperty("OPEN_AI_KEY"));
	}

	@Test
	void withAChrootLoadPropertiesStaysInsideIt() throws Exception {
		Class<?> probe = loadFromFolder();
		Path chroot = Files.createDirectories(this.folder.resolve("chroot"));
		SandboxedJavaExecution.setSandboxRootForCurrentThread(chroot.toString());

		Properties loaded = (Properties) call(probe, "loadSmss");

		assertNull(loaded.getProperty("OPEN_AI_KEY"));
	}

	@Test
	void jarReactorsGetTheSameSandbox() throws Exception {
		Class<?> probe = loadFromJar();

		assertNull(call(probe, "readEnv"));
		assertNull(call(probe, "readSecretSetting"));
		assertEquals("/opt/semosshome", call(probe, "readSetting"));
		assertInstanceOf(NoClassDefFoundError.class,
				assertThrows(InvocationTargetException.class, () -> call(probe, "touchDIHelper")).getCause());
		assertInstanceOf(NoClassDefFoundError.class,
				assertThrows(InvocationTargetException.class, () -> call(probe, "touchInternalUtility")).getCause());
		assertInstanceOf(SecurityException.class,
				assertThrows(InvocationTargetException.class, () -> call(probe, "readLocalProperty")).getCause());

		Path chroot = Files.createDirectories(this.folder.resolve("jar-chroot"));
		SandboxedJavaExecution.setSandboxRootForCurrentThread(chroot.toString());
		assertNull(((Properties) call(probe, "loadSmss")).getProperty("OPEN_AI_KEY"));
	}

	@Test
	void jarsCannotDefinePlatformPackageClasses() throws Exception {
		Path jar = this.folder.resolve("spoof.jar");
		writeJar(jar, "prerna/util/Spoofed", classNamed("prerna/util/Spoofed"));

		try (SemossProjectJarClassLoader loader = new SemossProjectJarClassLoader(new URL[] { jar.toUri().toURL() },
				new SemossClassLoader(testLoader()))) {
			assertThrows(ClassNotFoundException.class, () -> loader.loadClass("prerna.util.Spoofed"));
		}
	}

	@ParameterizedTest
	@ValueSource(strings = { "getAbsolutePath", "new" })
	void aMethodReferenceToARewrittenMethodRunsTheSandboxedVersion(String target) {
		Handle reference = "new".equals(target)
				? new Handle(Opcodes.H_NEWINVOKESPECIAL, "java/io/File", "<init>", "(Ljava/lang/String;)V", false)
				: new Handle(Opcodes.H_INVOKEVIRTUAL, "java/io/File", "getAbsolutePath", "()Ljava/lang/String;", false);
		byte[] original = classWith(mv -> mv.visitInvokeDynamicInsn("apply", "()Ljava/util/function/Function;",
				LAMBDA_METAFACTORY, Type.getType("(Ljava/lang/Object;)Ljava/lang/Object;"), reference,
				Type.getType("new".equals(target) ? "(Ljava/lang/String;)Ljava/io/File;"
						: "(Ljava/io/File;)Ljava/lang/String;")));

		List<Handle> handles = new ArrayList<>();
		List<String> calls = new ArrayList<>();
		new ClassReader(SemossClassLoader.applyBytecodeTransformations(original))
				.accept(new ClassVisitor(Opcodes.ASM9) {
					@Override
					public MethodVisitor visitMethod(int access, String name, String descriptor, String signature,
							String[] exceptions) {
						return new MethodVisitor(Opcodes.ASM9) {
							@Override
							public void visitMethodInsn(int opcode, String owner, String called,
									String calledDescriptor, boolean isInterface) {
								calls.add(owner);
							}

							@Override
							public void visitInvokeDynamicInsn(String indyName, String indyDescriptor, Handle bootstrap,
									Object... arguments) {
								for (Object argument : arguments) {
									if (argument instanceof Handle handle) {
										handles.add(handle);
									}
								}
							}
						};
					}
				}, 0);

		assertEquals(1, handles.size());
		assertEquals(PROBE, handles.get(0).getOwner());
		assertEquals(Opcodes.H_INVOKESTATIC, handles.get(0).getTag());
		assertTrue(calls.contains("prerna/util/SandboxedJavaExecution"), calls.toString());
		assertFalse(calls.contains("java/io/File"), calls.toString());
	}

	@Test
	void theSandboxedSettingGetterHidesWhatTheRdfMapEditorHides() {
		Properties rdfMap = DIHelper.getInstance().getCoreProp();
		rdfMap.setProperty("COUCH_CREDS", "admin:couch-password");
		rdfMap.setProperty("VAULT_ADDR", "https://vault.internal:8200");

		assertNull(SandboxedJavaExecution.sandboxedPlatformProperty("COUCH_CREDS"));
		assertNull(SandboxedJavaExecution.sandboxedPlatformProperty("VAULT_ADDR"));
		assertNull(SandboxedJavaExecution.sandboxedPlatformProperty(null));
		assertEquals("/opt/semosshome", SandboxedJavaExecution.sandboxedPlatformProperty(Constants.BASE_FOLDER));
	}

	@Test
	void aMethodHandleConstantToABlockedUtilityMethodIsRefused() {
		byte[] constant = classWith(mv -> mv.visitLdcInsn(new Handle(Opcodes.H_INVOKESTATIC, "prerna/util/Utility",
				"loadProperties", "(Ljava/lang/String;)Ljava/util/Properties;", false)));

		assertThrows(SecurityException.class, () -> SemossClassLoader.applyBytecodeTransformations(constant));
	}

	@Test
	void aLambdaWhoseBodyIsInTheClassIsAllowed() {
		byte[] lambda = classWith(mv -> mv.visitInvokeDynamicInsn("get", "()Ljava/util/function/Supplier;",
				LAMBDA_METAFACTORY, Type.getType("()Ljava/lang/Object;"),
				new Handle(Opcodes.H_INVOKESTATIC, PROBE, "lambda$0", "()Ljava/lang/Object;", false),
				Type.getType("()Ljava/lang/Object;")));

		assertTrue(SemossClassLoader.applyBytecodeTransformations(lambda).length > 0);
	}

	private Class<?> loadFromFolder() throws Exception {
		Path classFile = this.folder.resolve(PROBE + ".class");
		Files.createDirectories(classFile.getParent());
		Files.write(classFile, probeClass(engineSmss()));
		SemossClassLoader loader = new SemossClassLoader(testLoader());
		loader.setFolder(this.folder.toString());
		return loader.loadClass(PROBE.replace('/', '.'));
	}

	private Class<?> loadFromJar() throws Exception {
		Path jar = this.folder.resolve("reactors.jar");
		writeJar(jar, PROBE, probeClass(engineSmss()));
		SemossProjectJarClassLoader loader = new SemossProjectJarClassLoader(new URL[] { jar.toUri().toURL() },
				new SemossClassLoader(testLoader()));
		this.jarLoaders.add(loader);
		return loader.loadClass(PROBE.replace('/', '.'));
	}

	/**
	 * An engine smss outside any sandbox root, holding a secret.
	 */
	private String engineSmss() throws Exception {
		Path smss = this.folder.resolve("engines").resolve("Model__1234.smss");
		Files.createDirectories(smss.getParent());
		Files.writeString(smss, "OPEN_AI_KEY\tsk-live-engine-key\n");
		return smss.toAbsolutePath().toString();
	}

	private static Object call(Class<?> probe, String method) throws Exception {
		return probe.getMethod(method).invoke(null);
	}

	private static ClassLoader testLoader() {
		return SemossClassLoaderUnitTests.class.getClassLoader();
	}

	private static void writeJar(Path jar, String internalName, byte[] bytes) throws Exception {
		try (OutputStream file = Files.newOutputStream(jar); JarOutputStream out = new JarOutputStream(file)) {
			out.putNextEntry(new JarEntry(internalName + ".class"));
			out.write(bytes);
			out.closeEntry();
		}
	}

	/**
	 * A reactor-like class whose static methods each make one call the sandbox
	 * rewrites or blocks.
	 */
	private static byte[] probeClass(String smssPath) {
		ClassWriter writer = start(PROBE);
		method(writer, "touchDIHelper", mv -> mv.visitMethodInsn(Opcodes.INVOKESTATIC, "prerna/util/DIHelper",
				"getInstance", "()Lprerna/util/DIHelper;", false));
		method(writer, "findRedactor", mv -> {
			mv.visitLdcInsn("prerna.security.SecretRedactor");
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Class", "forName",
					"(Ljava/lang/String;)Ljava/lang/Class;", false);
		});
		method(writer, "readSetting", mv -> {
			mv.visitLdcInsn(Constants.BASE_FOLDER);
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, "prerna/util/Utility", "getDIHelperProperty",
					"(Ljava/lang/String;)Ljava/lang/String;", false);
		});
		method(writer, "readSecretSetting", mv -> {
			mv.visitLdcInsn("SMTP_PASSWORD");
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, "prerna/util/Utility", "getDIHelperProperty",
					"(Ljava/lang/String;)Ljava/lang/String;", false);
		});
		method(writer, "loadSmss", mv -> {
			mv.visitLdcInsn(smssPath);
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, "prerna/util/Utility", "loadProperties",
					"(Ljava/lang/String;)Ljava/util/Properties;", false);
		});
		method(writer, "touchInternalUtility", mv -> {
			mv.visitInsn(Opcodes.ACONST_NULL);
			mv.visitInsn(Opcodes.ACONST_NULL);
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, "prerna/util/InternalUtility", "loadProject",
					"(Ljava/lang/String;Ljava/util/Properties;)Lprerna/project/api/IProject;", false);
		});
		method(writer, "readLocalProperty", mv -> {
			mv.visitLdcInsn("core");
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, "prerna/util/Utility", "getDIHelperLocalProperty",
					"(Ljava/lang/String;)Ljava/lang/Object;", false);
		});
		method(writer, "readEnv", mv -> {
			mv.visitLdcInsn("PATH");
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/System", "getenv",
					"(Ljava/lang/String;)Ljava/lang/String;", false);
		});
		writer.visitEnd();
		return writer.toByteArray();
	}

	private static byte[] classWith(Consumer<MethodVisitor> body) {
		ClassWriter writer = start(PROBE);
		method(writer, "run", body);
		writer.visitEnd();
		return writer.toByteArray();
	}

	private static byte[] classNamed(String internalName) {
		ClassWriter writer = start(internalName);
		writer.visitEnd();
		return writer.toByteArray();
	}

	private static ClassWriter start(String internalName) {
		ClassWriter writer = new ClassWriter(ClassWriter.COMPUTE_MAXS);
		writer.visit(Opcodes.V21, Opcodes.ACC_PUBLIC | Opcodes.ACC_SUPER, internalName, null, "java/lang/Object", null);
		return writer;
	}

	private static void method(ClassWriter writer, String name, Consumer<MethodVisitor> body) {
		MethodVisitor mv = writer.visitMethod(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC, name, "()Ljava/lang/Object;",
				null, null);
		mv.visitCode();
		body.accept(mv);
		mv.visitInsn(Opcodes.ARETURN);
		mv.visitMaxs(0, 0);
		mv.visitEnd();
	}
}
