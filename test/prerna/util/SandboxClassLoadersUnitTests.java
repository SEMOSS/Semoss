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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.objectweb.asm.Handle;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

/**
 * One test per rule that keeps every class loader a project's custom reactors
 * can get or create inside the sandbox. The sandbox page in the docs lists each
 * rule with the test here that guards it.
 */
class SandboxClassLoadersUnitTests {

	private static final String CLASS_LOADER = "java/lang/ClassLoader";
	private static final String URL_CLASS_LOADER = "java/net/URLClassLoader";
	private static final String OWN_LOADER = "custom/sandbox/OwnLoader";
	private static final String URLS = "[Ljava/net/URL;";
	private static final String LOADER = "Ljava/lang/ClassLoader;";

	@TempDir
	Path folder;

	@AfterEach
	void clear() throws Exception {
		SandboxedJavaExecution.clearSandboxRootForCurrentThread();
		if (SandboxProbe.heldObject instanceof URLClassLoader held) {
			held.close();
		}
		SandboxProbe.heldLoader = null;
		SandboxProbe.heldObject = null;
	}

	@ParameterizedTest
	@ValueSource(strings = { "plain", "withParent", "withFactory", "named", "newInstance", "newInstanceWithParent" })
	void aNewUrlClassLoaderIsASandboxLoaderUnderTheProjectLoader(String form) throws Throwable {
		Class<?> probe = SandboxProbe.load(this.folder,
				new SandboxProbe()
						.method("plain", newUrlClassLoader("(" + URLS + ")V", SandboxClassLoadersUnitTests::noUrls))
						.method("withParent", newUrlClassLoader("(" + URLS + LOADER + ")V", mv -> {
							noUrls(mv);
							mv.visitInsn(Opcodes.ACONST_NULL);
						})).method("withFactory",
								newUrlClassLoader("(" + URLS + LOADER + "Ljava/net/URLStreamHandlerFactory;)V", mv -> {
									noUrls(mv);
									mv.visitInsn(Opcodes.ACONST_NULL);
									mv.visitInsn(Opcodes.ACONST_NULL);
								}))
						.method("named", newUrlClassLoader("(Ljava/lang/String;" + URLS + LOADER + ")V", mv -> {
							mv.visitLdcInsn("mine");
							noUrls(mv);
							mv.visitInsn(Opcodes.ACONST_NULL);
						})).method("newInstance", mv -> {
							noUrls(mv);
							mv.visitMethodInsn(Opcodes.INVOKESTATIC, URL_CLASS_LOADER, "newInstance",
									"(" + URLS + ")Ljava/net/URLClassLoader;", false);
						}).method("newInstanceWithParent", mv -> {
							noUrls(mv);
							mv.visitInsn(Opcodes.ACONST_NULL);
							mv.visitMethodInsn(Opcodes.INVOKESTATIC, URL_CLASS_LOADER, "newInstance",
									"(" + URLS + LOADER + ")Ljava/net/URLClassLoader;", false);
						}).bytes());

		try (URLClassLoader created = (URLClassLoader) SandboxProbe.run(probe, form)) {
			assertInstanceOf(SemossProjectJarClassLoader.class, created);
			assertSame(probe.getClassLoader(), created.getParent());
		}
	}

	@Test
	void aLoaderForARemoteJarIsRefused() throws Exception {
		Class<?> probe = SandboxProbe.load(this.folder,
				new SandboxProbe().method("run",
						newUrlClassLoader("(" + URLS + ")V", mv -> oneUrl(mv, "https://example.invalid/code.jar")))
						.bytes());

		assertThrows(SecurityException.class, () -> SandboxProbe.run(probe, "run"));
	}

	@Test
	void withAUserRootALoaderReadsJarsInsideIt() throws Throwable {
		Path root = Files.createDirectories(this.folder.resolve("user-root")).toRealPath();
		String outside = this.folder.toRealPath().resolve("outside.jar").toUri().toString();
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe()
				.method("run", newUrlClassLoader("(" + URLS + ")V", mv -> oneUrl(mv, outside))).bytes());
		SandboxedJavaExecution.setSandboxRootForCurrentThread(root.toString());

		try (URLClassLoader created = (URLClassLoader) SandboxProbe.run(probe, "run")) {
			assertTrue(Path.of(created.getURLs()[0].toURI()).startsWith(root));
		}
	}

	@Test
	void callsThatReturnALoaderReturnTheProjectsSandboxLoader() throws Throwable {
		SandboxProbe.heldLoader = ClassLoader.getSystemClassLoader();
		SandboxProbe.heldObject = SandboxClassLoadersUnitTests.class.getClassLoader();
		Class<?> probe = SandboxProbe.load(this.folder,
				new SandboxProbe()
						.method("system",
								SandboxProbe.call(Opcodes.INVOKESTATIC, CLASS_LOADER, "getSystemClassLoader",
										"()" + LOADER))
						.method("platform",
								SandboxProbe.call(Opcodes.INVOKESTATIC, CLASS_LOADER, "getPlatformClassLoader",
										"()" + LOADER))
						.method("ofPlatformClass", loaderOf("prerna/util/Utility")).method("parent", mv -> {
							loaderOf(SandboxProbe.INTERNAL_NAME).accept(mv);
							mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, CLASS_LOADER, "getParent", "()" + LOADER, false);
						}).method("fromField", mv -> mv.visitFieldInsn(Opcodes.GETSTATIC, "prerna/util/SandboxProbe",
								"heldLoader", LOADER))
						.method("fromCast", mv -> {
							mv.visitFieldInsn(Opcodes.GETSTATIC, "prerna/util/SandboxProbe", "heldObject",
									"Ljava/lang/Object;");
							mv.visitTypeInsn(Opcodes.CHECKCAST, CLASS_LOADER);
						}).method("ofJdkClass", loaderOf("java/lang/String")).bytes());

		for (String way : new String[] { "system", "platform", "ofPlatformClass", "parent", "fromField", "fromCast" }) {
			assertSame(probe.getClassLoader(), SandboxProbe.run(probe, way), way);
		}
		assertNull(SandboxProbe.run(probe, "ofJdkClass"));
	}

	@Test
	void aMethodReferenceToALoaderCallReturnsTheSandboxLoader() throws Throwable {
		Handle metafactory = new Handle(Opcodes.H_INVOKESTATIC, "java/lang/invoke/LambdaMetafactory", "metafactory",
				"(Ljava/lang/invoke/MethodHandles$Lookup;Ljava/lang/String;Ljava/lang/invoke/MethodType;"
						+ "Ljava/lang/invoke/MethodType;Ljava/lang/invoke/MethodHandle;Ljava/lang/invoke/MethodType;)"
						+ "Ljava/lang/invoke/CallSite;",
				false);
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("run", mv -> {
			mv.visitInvokeDynamicInsn("apply", "()Ljava/util/function/Function;", metafactory,
					Type.getType("(Ljava/lang/Object;)Ljava/lang/Object;"),
					new Handle(Opcodes.H_INVOKEVIRTUAL, "java/lang/Class", "getClassLoader", "()" + LOADER, false),
					Type.getType("(Ljava/lang/Class;)" + LOADER));
			mv.visitLdcInsn(Type.getObjectType("prerna/util/Utility"));
			mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, "java/util/function/Function", "apply",
					"(Ljava/lang/Object;)Ljava/lang/Object;", true);
		}).bytes());

		assertSame(probe.getClassLoader(), SandboxProbe.run(probe, "run"));
	}

	@Test
	void aPlatformLoaderCastToAUrlClassLoaderIsRefused() throws Exception {
		SandboxProbe.heldObject = new URLClassLoader(new URL[0]);
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("run", mv -> {
			mv.visitFieldInsn(Opcodes.GETSTATIC, "prerna/util/SandboxProbe", "heldObject", "Ljava/lang/Object;");
			mv.visitTypeInsn(Opcodes.CHECKCAST, URL_CLASS_LOADER);
		}).bytes());

		assertThrows(SecurityException.class, () -> SandboxProbe.run(probe, "run"));
	}

	@Test
	void forNameFindsTheProjectsOwnClasses() throws Throwable {
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("self", mv -> {
			mv.visitLdcInsn(SandboxProbe.INTERNAL_NAME.replace('/', '.'));
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Class", "forName",
					"(Ljava/lang/String;)Ljava/lang/Class;", false);
		}).bytes());

		assertSame(probe, SandboxProbe.run(probe, "self"));
	}

	@Test
	void loadClassOnALoaderAppliesTheBlockList() throws Throwable {
		Class<?> probe = SandboxProbe.load(this.folder,
				new SandboxProbe().method("blocked", loadByName("prerna.util.DIHelper"))
						.method("open", loadByName("prerna.util.Utility")).bytes());

		assertThrows(SecurityException.class, () -> SandboxProbe.run(probe, "blocked"));
		assertSame(Utility.class, SandboxProbe.run(probe, "open"));
	}

	@Test
	void aCustomClassLoaderIsMovedUnderTheSandbox() throws Throwable {
		SemossClassLoader sandbox = ownLoaderIn(this.folder, CLASS_LOADER);
		Class<?> ownLoaderClass = sandbox.loadClass(OWN_LOADER.replace('/', '.'));

		Object own = ownLoaderClass.getConstructor().newInstance();
		Object withAnotherParent = ownLoaderClass.getConstructor(ClassLoader.class)
				.newInstance(ClassLoader.getSystemClassLoader());

		assertInstanceOf(SandboxedClassLoader.class, own);
		assertSame(sandbox, ((ClassLoader) own).getParent());
		assertSame(sandbox, ((ClassLoader) withAnotherParent).getParent());
	}

	@Test
	void classesACustomClassLoaderDefinesAreSandboxed() throws Throwable {
		SemossClassLoader sandbox = ownLoaderIn(this.folder, "java/security/SecureClassLoader");
		Class<?> ownLoaderClass = sandbox.loadClass(OWN_LOADER.replace('/', '.'));
		Object own = ownLoaderClass.getConstructor().newInstance();
		byte[] definedBytes = new SandboxProbe("custom/sandbox/Defined", "java/lang/Object")
				.method("readEnv",
						SandboxProbe.call(Opcodes.INVOKESTATIC, "java/lang/System", "getenv",
								"(Ljava/lang/String;)Ljava/lang/String;"))
				.method("nameDIHelper", mv -> mv.visitLdcInsn(Type.getObjectType("prerna/util/DIHelper"))).bytes();

		Class<?> defined = (Class<?>) ownLoaderClass.getMethod("define", byte[].class).invoke(own, definedBytes);

		assertSame(own, defined.getClassLoader());
		assertNull(SandboxProbe.run(defined, "readEnv"));
		assertThrows(NoClassDefFoundError.class, () -> SandboxProbe.run(defined, "nameDIHelper"));
	}

	@Test
	void aCustomClassLoaderCannotDefinePlatformPackageClasses() throws Exception {
		SemossClassLoader sandbox = ownLoaderIn(this.folder, CLASS_LOADER);
		Class<?> ownLoaderClass = sandbox.loadClass(OWN_LOADER.replace('/', '.'));
		Object own = ownLoaderClass.getConstructor().newInstance();
		byte[] spoofed = new SandboxProbe("prerna/util/Spoofed", "java/lang/Object").bytes();

		InvocationTargetException refused = assertThrows(InvocationTargetException.class,
				() -> ownLoaderClass.getMethod("define", byte[].class).invoke(own, spoofed));
		assertInstanceOf(SecurityException.class, refused.getCause());
	}

	@ParameterizedTest
	@ValueSource(strings = { "(Ljava/lang/String;)Ljava/lang/Class;", "(Ljava/lang/String;Z)Ljava/lang/Class;" })
	void aCustomClassLoaderCannotOverrideLoadClass(String descriptor) throws Exception {
		SandboxProbe.write(this.folder, OWN_LOADER,
				new SandboxProbe(OWN_LOADER, CLASS_LOADER).constructor("()V", superConstructor(CLASS_LOADER, "()V"))
						.instanceMethod("loadClass", descriptor, mv -> mv.visitInsn(Opcodes.ACONST_NULL)).bytes());

		ClassNotFoundException refused = assertThrows(ClassNotFoundException.class,
				() -> SandboxProbe.loader(this.folder).loadClass(OWN_LOADER.replace('/', '.')));
		assertInstanceOf(SecurityException.class, refused.getCause());
	}

	@Test
	void aCustomUrlClassLoaderIsMovedUnderTheSandbox() throws Throwable {
		SandboxProbe.write(this.folder, OWN_LOADER,
				new SandboxProbe(OWN_LOADER, URL_CLASS_LOADER).constructor("(" + URLS + LOADER + ")V", mv -> {
					mv.visitVarInsn(Opcodes.ALOAD, 0);
					mv.visitVarInsn(Opcodes.ALOAD, 1);
					mv.visitVarInsn(Opcodes.ALOAD, 2);
					mv.visitMethodInsn(Opcodes.INVOKESPECIAL, URL_CLASS_LOADER, "<init>", "(" + URLS + LOADER + ")V",
							false);
				}).bytes());
		SemossClassLoader sandbox = SandboxProbe.loader(this.folder);
		Class<?> ownLoaderClass = sandbox.loadClass(OWN_LOADER.replace('/', '.'));

		try (URLClassLoader own = (URLClassLoader) ownLoaderClass.getConstructor(URL[].class, ClassLoader.class)
				.newInstance(new URL[0], ClassLoader.getSystemClassLoader())) {
			assertInstanceOf(SemossProjectJarClassLoader.class, own);
			assertSame(sandbox, own.getParent());
		}
	}

	@ParameterizedTest
	@ValueSource(strings = { "prerna/util/SandboxedJavaExecution", "prerna/util/SandboxedXml",
			"prerna/util/SemossClassLoader", "prerna/util/SemossProjectJarClassLoader",
			"prerna/util/SemossJarClassLoader", "prerna/util/SandboxedClassLoader", "prerna/util/SandboxedPdfBox",
			"prerna/util/SandboxedHikari", "prerna/util/SandboxedJackson", "prerna/util/SandboxedLog4j",
			"prerna/util/SandboxedPoi", "prerna/util/SandboxedMail" })
	void customCodeCannotNameTheSandboxClasses(String sandboxClass) {
		byte[] calls = new SandboxProbe().method("run",
				SandboxProbe.call(Opcodes.INVOKESTATIC, sandboxClass, "clearSandboxRootForCurrentThread", "()V"))
				.bytes();
		byte[] creates = new SandboxProbe().method("run", mv -> {
			mv.visitTypeInsn(Opcodes.NEW, sandboxClass);
			mv.visitInsn(Opcodes.POP);
			mv.visitInsn(Opcodes.ACONST_NULL);
		}).bytes();
		byte[] reads = new SandboxProbe()
				.method("run",
						mv -> mv.visitFieldInsn(Opcodes.GETSTATIC, sandboxClass, "anything", "Ljava/lang/Object;"))
				.bytes();
		byte[] references = new SandboxProbe().method("run", mv -> mv.visitLdcInsn(
				new Handle(Opcodes.H_INVOKESTATIC, sandboxClass, "clearSandboxRootForCurrentThread", "()V", false)))
				.bytes();
		byte[] extended = new SandboxProbe(OWN_LOADER, sandboxClass).bytes();

		for (byte[] refused : new byte[][] { calls, creates, reads, references, extended }) {
			SecurityException error = assertThrows(SecurityException.class,
					() -> SemossClassLoader.applyBytecodeTransformations(refused));
			assertEquals(sandboxClass.replace('/', '.') + " is not permitted in project reactors", error.getMessage());
		}
	}

	@Test
	void aMethodHandleConstantToALoaderCallIsRefused() {
		Handle[] references = {
				new Handle(Opcodes.H_INVOKESTATIC, CLASS_LOADER, "getSystemClassLoader", "()" + LOADER, false),
				new Handle(Opcodes.H_INVOKEVIRTUAL, "java/lang/Class", "getClassLoader", "()" + LOADER, false),
				new Handle(Opcodes.H_INVOKESTATIC, "java/lang/Class", "forName",
						"(Ljava/lang/String;)Ljava/lang/Class;", false),
				new Handle(Opcodes.H_INVOKEVIRTUAL, CLASS_LOADER, "loadClass", "(Ljava/lang/String;)Ljava/lang/Class;",
						false),
				new Handle(Opcodes.H_NEWINVOKESPECIAL, URL_CLASS_LOADER, "<init>", "(" + URLS + ")V", false),
				new Handle(Opcodes.H_INVOKESTATIC, URL_CLASS_LOADER, "newInstance",
						"(" + URLS + ")Ljava/net/URLClassLoader;", false) };

		for (Handle reference : references) {
			byte[] constant = new SandboxProbe().method("run", mv -> mv.visitLdcInsn(reference)).bytes();

			assertThrows(SecurityException.class, () -> SemossClassLoader.applyBytecodeTransformations(constant),
					reference.toString());
		}
	}

	/**
	 * Puts a project class loader in the folder: it extends the given JDK loader
	 * class, takes an optional parent, and has a {@code define} method that calls
	 * {@code defineClass} on the bytes it is given.
	 */
	private static SemossClassLoader ownLoaderIn(Path folder, String superName) throws Exception {
		SandboxProbe.write(folder, OWN_LOADER, new SandboxProbe(OWN_LOADER, superName)
				.constructor("()V", superConstructor(superName, "()V")).constructor("(" + LOADER + ")V", mv -> {
					mv.visitVarInsn(Opcodes.ALOAD, 0);
					mv.visitVarInsn(Opcodes.ALOAD, 1);
					mv.visitMethodInsn(Opcodes.INVOKESPECIAL, superName, "<init>", "(" + LOADER + ")V", false);
				}).instanceMethod("define", "([B)Ljava/lang/Class;", mv -> {
					mv.visitVarInsn(Opcodes.ALOAD, 0);
					mv.visitInsn(Opcodes.ACONST_NULL);
					mv.visitVarInsn(Opcodes.ALOAD, 1);
					mv.visitInsn(Opcodes.ICONST_0);
					mv.visitVarInsn(Opcodes.ALOAD, 1);
					mv.visitInsn(Opcodes.ARRAYLENGTH);
					mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, OWN_LOADER, "defineClass",
							"(Ljava/lang/String;[BII)Ljava/lang/Class;", false);
				}).bytes());
		return SandboxProbe.loader(folder);
	}

	private static Consumer<MethodVisitor> superConstructor(String superName, String descriptor) {
		return mv -> {
			mv.visitVarInsn(Opcodes.ALOAD, 0);
			mv.visitMethodInsn(Opcodes.INVOKESPECIAL, superName, "<init>", descriptor, false);
		};
	}

	private static Consumer<MethodVisitor> newUrlClassLoader(String descriptor, Consumer<MethodVisitor> arguments) {
		return mv -> {
			mv.visitTypeInsn(Opcodes.NEW, URL_CLASS_LOADER);
			mv.visitInsn(Opcodes.DUP);
			arguments.accept(mv);
			mv.visitMethodInsn(Opcodes.INVOKESPECIAL, URL_CLASS_LOADER, "<init>", descriptor, false);
		};
	}

	private static void noUrls(MethodVisitor mv) {
		mv.visitInsn(Opcodes.ICONST_0);
		mv.visitTypeInsn(Opcodes.ANEWARRAY, "java/net/URL");
	}

	private static void oneUrl(MethodVisitor mv, String url) {
		mv.visitInsn(Opcodes.ICONST_1);
		mv.visitTypeInsn(Opcodes.ANEWARRAY, "java/net/URL");
		mv.visitInsn(Opcodes.DUP);
		mv.visitInsn(Opcodes.ICONST_0);
		mv.visitTypeInsn(Opcodes.NEW, "java/net/URL");
		mv.visitInsn(Opcodes.DUP);
		mv.visitLdcInsn(url);
		mv.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/net/URL", "<init>", "(Ljava/lang/String;)V", false);
		mv.visitInsn(Opcodes.AASTORE);
	}

	private static Consumer<MethodVisitor> loaderOf(String internalName) {
		return mv -> {
			mv.visitLdcInsn(Type.getObjectType(internalName));
			mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Class", "getClassLoader", "()" + LOADER, false);
		};
	}

	private static Consumer<MethodVisitor> loadByName(String className) {
		return mv -> {
			loaderOf(SandboxProbe.INTERNAL_NAME).accept(mv);
			mv.visitLdcInsn(className);
			mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, CLASS_LOADER, "loadClass",
					"(Ljava/lang/String;)Ljava/lang/Class;", false);
		};
	}
}
