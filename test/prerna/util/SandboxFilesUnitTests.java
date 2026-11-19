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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.objectweb.asm.Handle;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import jakarta.activation.FileDataSource;

/**
 * One test per rule that keeps the files a project's custom reactors open
 * inside the user's folder when chroot is on. Each test has a file outside the
 * folder holding "outside" and a file at the same path inside the folder
 * holding "inside", and checks which one the project code reaches. The sandbox
 * page in the docs lists each rule with the test here that guards it.
 */
class SandboxFilesUnitTests {

	private static final String SANDBOXED_PATH = "prerna/util/SandboxedJavaExecution.sandboxedPath";
	private static final String PATH = "java/nio/file/Path";

	@TempDir
	Path folder;

	private Path root;
	private Path outside;
	private Path inside;
	private Properties savedCoreProp;

	@BeforeEach
	void makeFiles() throws Exception {
		this.savedCoreProp = DIHelper.getInstance().getCoreProp();
		DIHelper.getInstance().setCoreProp(new Properties());
		Path base = this.folder.toRealPath();
		this.root = Files.createDirectories(base.resolve("user-root"));
		this.outside = Files.writeString(base.resolve("outside.txt"), "outside");
		this.inside = insideCopyOf(this.outside);
		Files.writeString(this.inside, "inside");
	}

	@AfterEach
	void clear() {
		SandboxedJavaExecution.clearSandboxRootForCurrentThread();
		DIHelper.getInstance().setCoreProp(this.savedCoreProp);
	}

	@ParameterizedTest
	@CsvSource({ "java/io/FileReader, (Ljava/lang/String;)V",
			"java/io/FileReader, (Ljava/lang/String;Ljava/nio/charset/Charset;)V",
			"java/io/FileWriter, (Ljava/lang/String;)V", "java/io/FileWriter, (Ljava/lang/String;Z)V",
			"java/io/FileWriter, (Ljava/lang/String;Ljava/nio/charset/Charset;)V",
			"java/io/FileWriter, (Ljava/lang/String;Ljava/nio/charset/Charset;Z)V",
			"java/io/RandomAccessFile, (Ljava/lang/String;Ljava/lang/String;)V",
			"java/io/PrintWriter, (Ljava/lang/String;)V",
			"java/io/PrintWriter, (Ljava/lang/String;Ljava/lang/String;)V",
			"java/io/PrintWriter, (Ljava/lang/String;Ljava/nio/charset/Charset;)V",
			"java/io/PrintStream, (Ljava/lang/String;)V",
			"java/io/PrintStream, (Ljava/lang/String;Ljava/lang/String;)V",
			"java/io/PrintStream, (Ljava/lang/String;Ljava/nio/charset/Charset;)V",
			"java/util/Formatter, (Ljava/lang/String;)V",
			"java/util/Formatter, (Ljava/lang/String;Ljava/lang/String;)V",
			"java/util/Formatter, (Ljava/lang/String;Ljava/lang/String;Ljava/util/Locale;)V",
			"java/util/Formatter, (Ljava/lang/String;Ljava/nio/charset/Charset;Ljava/util/Locale;)V",
			"java/util/zip/ZipFile, (Ljava/lang/String;)V",
			"java/util/zip/ZipFile, (Ljava/lang/String;Ljava/nio/charset/Charset;)V",
			"java/util/jar/JarFile, (Ljava/lang/String;)V", "java/util/jar/JarFile, (Ljava/lang/String;Z)V" })
	void aFileOpenedByNameIsTheOneInsideTheUsersFolder(String owner, String descriptor) throws Throwable {
		boolean zip = owner.endsWith("ZipFile") || owner.endsWith("JarFile");
		if (zip) {
			writeZip(this.outside);
			writeZip(this.inside);
		}
		byte[] outsideBefore = Files.readAllBytes(this.outside);
		byte[] original = new SandboxProbe().method("run", openByName(owner, descriptor, this.outside.toString()))
				.bytes();
		Class<?> probe = SandboxProbe.load(this.folder, original);
		SandboxedJavaExecution.setSandboxRootForCurrentThread(this.root.toString());

		Object opened = SandboxProbe.run(probe, "run");

		assertTrue(
				SandboxProbe.calls(SemossClassLoader.applyBytecodeTransformations(original)).contains(SANDBOXED_PATH));
		if (opened instanceof Reader reader) {
			assertEquals('i', (char) reader.read());
		}
		if (opened instanceof ZipFile zipFile) {
			assertTrue(Path.of(zipFile.getName()).startsWith(this.root), zipFile.getName());
		}
		((AutoCloseable) opened).close();
		assertArrayEquals(outsideBefore, Files.readAllBytes(this.outside));
	}

	@Test
	void withNoUsersFolderAFileOpenedByNameIsTheOneNamed() throws Throwable {
		Class<?> probe = SandboxProbe.load(this.folder,
				new SandboxProbe()
						.method("run",
								openByName("java/io/FileReader", "(Ljava/lang/String;)V", this.outside.toString()))
						.bytes());

		try (Reader reader = (Reader) SandboxProbe.run(probe, "run")) {
			assertEquals('o', (char) reader.read());
		}
	}

	@Test
	void aPathWorkedOutByNavigationIsCheckedWhereItIsUsed() throws Throwable {
		Path doomedOutside = Files.writeString(this.outside.resolveSibling("doomed.txt"), "outside");
		Path doomedInside = Files.writeString(insideCopyOf(doomedOutside), "inside");
		Path movedOutside = this.outside.resolveSibling("moved.txt");
		Path source = Files.writeString(this.root.resolve("source.txt"), "moved");
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("readString", mv -> {
			escapingPath("../outside.txt").accept(mv);
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/nio/file/Files", "readString",
					"(Ljava/nio/file/Path;)Ljava/lang/String;", false);
		}).method("reader", mv -> {
			mv.visitTypeInsn(Opcodes.NEW, "java/io/FileReader");
			mv.visitInsn(Opcodes.DUP);
			escapingFile("../outside.txt").accept(mv);
			mv.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/io/FileReader", "<init>", "(Ljava/io/File;)V", false);
		}).method("delete", mv -> {
			escapingFile("../doomed.txt").accept(mv);
			mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/io/File", "delete", "()Z", false);
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Boolean", "valueOf", "(Z)Ljava/lang/Boolean;", false);
		}).method("rename", mv -> {
			mv.visitTypeInsn(Opcodes.NEW, "java/io/File");
			mv.visitInsn(Opcodes.DUP);
			mv.visitLdcInsn(source.toString());
			mv.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/io/File", "<init>", "(Ljava/lang/String;)V", false);
			escapingFile("../moved.txt").accept(mv);
			mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/io/File", "renameTo", "(Ljava/io/File;)Z", false);
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Boolean", "valueOf", "(Z)Ljava/lang/Boolean;", false);
		}).bytes());
		SandboxedJavaExecution.setSandboxRootForCurrentThread(this.root.toString());

		assertEquals("inside", SandboxProbe.run(probe, "readString"));
		try (Reader reader = (Reader) SandboxProbe.run(probe, "reader")) {
			assertEquals('i', (char) reader.read());
		}
		assertEquals(Boolean.TRUE, SandboxProbe.run(probe, "delete"));
		assertTrue(Files.exists(doomedOutside));
		assertFalse(Files.exists(doomedInside));
		assertEquals(Boolean.TRUE, SandboxProbe.run(probe, "rename"));
		assertFalse(Files.exists(movedOutside));
		assertEquals("moved", Files.readString(insideCopyOf(movedOutside)));
	}

	@Test
	void aMethodReferenceToAFileCallFollowsTheSameRule() throws Throwable {
		Path doomedOutside = Files.writeString(this.outside.resolveSibling("doomed.txt"), "outside");
		Path doomedInside = Files.writeString(insideCopyOf(doomedOutside), "inside");
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("readString", mv -> {
			reference(mv, "apply", "()Ljava/util/function/Function;", "(Ljava/lang/Object;)Ljava/lang/Object;",
					new Handle(Opcodes.H_INVOKESTATIC, "java/nio/file/Files", "readString",
							"(Ljava/nio/file/Path;)Ljava/lang/String;", false),
					"(Ljava/nio/file/Path;)Ljava/lang/String;");
			escapingPath("../outside.txt").accept(mv);
			mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, "java/util/function/Function", "apply",
					"(Ljava/lang/Object;)Ljava/lang/Object;", true);
		}).method("delete", mv -> {
			reference(mv, "test", "()Ljava/util/function/Predicate;", "(Ljava/lang/Object;)Z",
					new Handle(Opcodes.H_INVOKEVIRTUAL, "java/io/File", "delete", "()Z", false), "(Ljava/io/File;)Z");
			escapingFile("../doomed.txt").accept(mv);
			mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, "java/util/function/Predicate", "test", "(Ljava/lang/Object;)Z",
					true);
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Boolean", "valueOf", "(Z)Ljava/lang/Boolean;", false);
		}).method("reader", mv -> {
			reference(mv, "apply", "()Ljava/util/function/Function;", "(Ljava/lang/Object;)Ljava/lang/Object;",
					new Handle(Opcodes.H_NEWINVOKESPECIAL, "java/io/FileReader", "<init>", "(Ljava/lang/String;)V",
							false),
					"(Ljava/lang/String;)Ljava/io/FileReader;");
			mv.visitLdcInsn(this.outside.toString());
			mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, "java/util/function/Function", "apply",
					"(Ljava/lang/Object;)Ljava/lang/Object;", true);
		}).bytes());
		SandboxedJavaExecution.setSandboxRootForCurrentThread(this.root.toString());

		assertEquals("inside", SandboxProbe.run(probe, "readString"));
		assertEquals(Boolean.TRUE, SandboxProbe.run(probe, "delete"));
		assertTrue(Files.exists(doomedOutside));
		assertFalse(Files.exists(doomedInside));
		try (Reader reader = (Reader) SandboxProbe.run(probe, "reader")) {
			assertEquals('i', (char) reader.read());
		}
	}

	@Test
	void workingOutAPathIsLeftAlone() throws Throwable {
		String directory = this.root.resolve("dir").toString();
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("run", mv -> {
			path(directory).accept(mv);
			path(directory + File.separator + "file.txt").accept(mv);
			mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, PATH, "getFileName", "()Ljava/nio/file/Path;", true);
			mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, PATH, "resolve", "(Ljava/nio/file/Path;)Ljava/nio/file/Path;",
					true);
		}).bytes());
		SandboxedJavaExecution.setSandboxRootForCurrentThread(this.root.toString());

		assertEquals(this.root.resolve("dir").resolve("file.txt"), SandboxProbe.run(probe, "run"));
	}

	@Test
	void aFileHandedToASharedLibraryIsTheOneInsideTheUsersFolder() throws Throwable {
		Path outsideJson = Files.writeString(this.outside.resolveSibling("data.json"), "{\"where\":\"outside\"}");
		Files.writeString(insideCopyOf(outsideJson), "{\"where\":\"inside\"}");
		String mapper = "com/fasterxml/jackson/databind/ObjectMapper";
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("readTree", mv -> {
			mv.visitTypeInsn(Opcodes.NEW, mapper);
			mv.visitInsn(Opcodes.DUP);
			mv.visitMethodInsn(Opcodes.INVOKESPECIAL, mapper, "<init>", "()V", false);
			escapingFile("../data.json").accept(mv);
			mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, mapper, "readTree",
					"(Ljava/io/File;)Lcom/fasterxml/jackson/databind/JsonNode;", false);
			mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "com/fasterxml/jackson/databind/JsonNode", "toString",
					"()Ljava/lang/String;", false);
		}).method("dataSource", mv -> {
			mv.visitTypeInsn(Opcodes.NEW, "jakarta/activation/FileDataSource");
			mv.visitInsn(Opcodes.DUP);
			mv.visitLdcInsn(this.outside.toString());
			mv.visitMethodInsn(Opcodes.INVOKESPECIAL, "jakarta/activation/FileDataSource", "<init>",
					"(Ljava/lang/String;)V", false);
		}).bytes());
		SandboxedJavaExecution.setSandboxRootForCurrentThread(this.root.toString());

		assertEquals("{\"where\":\"inside\"}", SandboxProbe.run(probe, "readTree"));
		FileDataSource source = assertInstanceOf(FileDataSource.class, SandboxProbe.run(probe, "dataSource"));
		assertTrue(source.getFile().toPath().startsWith(this.root));
	}

	@Test
	void aPlatformHelperThatTakesAPathChecksTheUsersFolder() throws Throwable {
		Consumer<MethodVisitor> folderIsNotEmpty = mv -> {
			mv.visitLdcInsn(this.outside.getParent().toString());
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, "prerna/util/Utility", "folderIsNotEmpty", "(Ljava/lang/String;)Z",
					false);
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Boolean", "valueOf", "(Z)Ljava/lang/Boolean;", false);
		};
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("run", folderIsNotEmpty).bytes());

		assertEquals(Boolean.TRUE, SandboxProbe.run(probe, "run"));
		Files.delete(this.inside);
		SandboxedJavaExecution.setSandboxRootForCurrentThread(this.root.toString());
		assertEquals(Boolean.FALSE, SandboxProbe.run(probe, "run"));
	}

	@ParameterizedTest
	@CsvSource({
			"prerna/util/Utility, writeResultToFile, (Ljava/lang/String;Ljava/util/Iterator;Ljava/lang/String;)Ljava/io/File;, true",
			"prerna/util/Utility, writeResultToJson, (Ljava/lang/String;Ljava/util/Iterator;)Ljava/io/File;, true",
			"prerna/util/Utility, writeWorkbook, (Lorg/apache/poi/ss/usermodel/Workbook;Ljava/lang/String;)V, true",
			"prerna/util/Utility, folderHasAnyFiles, (Ljava/lang/String;)Z, true",
			"prerna/util/Utility, getUniqueFilePath, (Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;, true",
			"org/apache/poi/openxml4j/opc/OPCPackage, open, (Ljava/lang/String;)Lorg/apache/poi/openxml4j/opc/OPCPackage;, true",
			"org/apache/poi/openxml4j/opc/OPCPackage, create, (Ljava/lang/String;)Lorg/apache/poi/openxml4j/opc/OPCPackage;, true",
			"org/apache/poi/ooxml/POIXMLDocument, openPackage, (Ljava/lang/String;)Lorg/apache/poi/openxml4j/opc/OPCPackage;, true",
			"jakarta/mail/internet/MimeBodyPart, attachFile, (Ljava/lang/String;)V, false",
			"jakarta/mail/internet/MimeBodyPart, saveFile, (Ljava/lang/String;)V, false" })
	void aCallThatTakesAPathAsTextGetsThePathRule(String owner, String name, String descriptor, boolean isStatic) {
		int opcode = isStatic ? Opcodes.INVOKESTATIC : Opcodes.INVOKEVIRTUAL;
		byte[] original = new SandboxProbe().method("run", SandboxProbe.call(opcode, owner, name, descriptor)).bytes();

		List<String> calls = SandboxProbe.calls(SemossClassLoader.applyBytecodeTransformations(original));

		assertTrue(calls.contains(SANDBOXED_PATH), calls.toString());
		assertTrue(calls.indexOf(SANDBOXED_PATH) < calls.indexOf(owner + "." + name), calls.toString());
	}

	@Test
	void aZipFileSystemOpenedByItsAddressIsTheOneInsideTheUsersFolder() throws Throwable {
		Path zipOutside = this.outside.resolveSibling("outside.zip");
		try (ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(zipOutside))) {
			zip.putNextEntry(new ZipEntry("entry.txt"));
			zip.write("outside".getBytes(StandardCharsets.UTF_8));
			zip.closeEntry();
		}
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("open", mv -> {
			mv.visitFieldInsn(Opcodes.GETSTATIC, "prerna/util/SandboxProbe", "heldObject", "Ljava/lang/Object;");
			mv.visitTypeInsn(Opcodes.CHECKCAST, "java/net/URI");
			mv.visitTypeInsn(Opcodes.NEW, "java/util/HashMap");
			mv.visitInsn(Opcodes.DUP);
			mv.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/util/HashMap", "<init>", "()V", false);
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/nio/file/FileSystems", "newFileSystem",
					"(Ljava/net/URI;Ljava/util/Map;)Ljava/nio/file/FileSystem;", false);
		}).bytes());
		SandboxedJavaExecution.setSandboxRootForCurrentThread(this.root.toString());
		try {
			SandboxProbe.heldObject = java.net.URI.create("jar:" + zipOutside.toUri());
			assertThrows(java.io.IOException.class, () -> SandboxProbe.run(probe, "open"));
			SandboxProbe.heldObject = java.net.URI.create("http://example.com/archive.zip");
			assertThrows(SecurityException.class, () -> SandboxProbe.run(probe, "open"));
		} finally {
			SandboxProbe.heldObject = null;
		}
	}

	@ParameterizedTest
	@CsvSource({ "org/apache/poi/xssf/usermodel/XSSFWorkbook", "jakarta/activation/MimetypesFileTypeMap",
			"jakarta/activation/MailcapCommandMap", "java/io/FileInputStream", "java/io/FileOutputStream" })
	void aSubclassThatOpensAFileByNameGetsThePathRule(String superName) {
		byte[] subclass = new SandboxProbe("custom/sandbox/Opener", superName)
				.constructor("(Ljava/lang/String;)V", mv -> {
					mv.visitVarInsn(Opcodes.ALOAD, 0);
					mv.visitVarInsn(Opcodes.ALOAD, 1);
					mv.visitMethodInsn(Opcodes.INVOKESPECIAL, superName, "<init>", "(Ljava/lang/String;)V", false);
				}).bytes();

		assertTrue(
				SandboxProbe.calls(SemossClassLoader.applyBytecodeTransformations(subclass)).contains(SANDBOXED_PATH));
	}

	@Test
	void aProjectClassCannotExtendFile() {
		byte[] subclass = new SandboxProbe("custom/sandbox/OwnFile", "java/io/File").bytes();

		assertThrows(SecurityException.class, () -> SemossClassLoader.applyBytecodeTransformations(subclass));
	}

	@Test
	void withChrootOnAThreadWithoutTheUsersFolderCannotOpenFiles() throws Throwable {
		DIHelper.getInstance().getCoreProp().setProperty(Constants.CHROOT_ENABLE, "true");
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("run", newFile("notes.txt")).bytes());
		SandboxedJavaExecution.setSandboxRootForCurrentThread(this.root.toString());
		AtomicReference<Throwable> onAnotherThread = new AtomicReference<>();

		Thread other = new Thread(() -> {
			try {
				SandboxProbe.run(probe, "run");
			} catch (Throwable failure) {
				onAnotherThread.set(failure);
			}
		});
		other.start();
		other.join();

		assertEquals(this.root.resolve("notes.txt"), ((File) SandboxProbe.run(probe, "run")).toPath());
		assertInstanceOf(SecurityException.class, onAnotherThread.get());
	}

	@Test
	void withChrootOffAThreadWithoutAFolderOpensFilesAsNamed() throws Throwable {
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("run", newFile("notes.txt")).bytes());

		assertEquals(new File("notes.txt"), SandboxProbe.run(probe, "run"));
	}

	@Test
	void withChrootOffAReactorStillRunsAndThePlatformWarns() throws Throwable {
		Class<?> probe = SandboxProbe.load(this.folder,
				new SandboxProbe()
						.method("run",
								openByName("java/io/FileReader", "(Ljava/lang/String;)V", this.outside.toString()))
						.bytes());
		org.apache.logging.log4j.core.Logger logger = (org.apache.logging.log4j.core.Logger) LogManager
				.getLogger(SandboxedJavaExecution.class);
		Level levelBefore = logger.getLevel();
		List<String> logged = new ArrayList<>();
		AbstractAppender capture = new AbstractAppender("chroot-warning", null, null, true, Property.EMPTY_ARRAY) {
			@Override
			public void append(LogEvent event) {
				logged.add(event.getMessage().getFormattedMessage());
			}
		};
		capture.start();
		logger.addAppender(capture);
		logger.setLevel(Level.WARN);
		SandboxedJavaExecution.CHROOT_OFF_LAST_WARNED.set(0);
		try {
			try (Reader reader = (Reader) SandboxProbe.run(probe, "run")) {
				assertEquals('o', (char) reader.read());
			}
			try (Reader reader = (Reader) SandboxProbe.run(probe, "run")) {
				assertEquals('o', (char) reader.read());
			}
		} finally {
			logger.removeAppender(capture);
			logger.setLevel(levelBefore);
		}

		assertEquals(1, logged.size(), logged.toString());
		assertTrue(logged.get(0).contains("This would typically require CHROOT_ENABLE to be enabled for security"),
				logged.get(0));
	}

	/**
	 * Where a file outside the user's folder is found inside it: the same path,
	 * taken from the folder instead of from the root of the disk.
	 */
	private Path insideCopyOf(Path outsideFile) throws Exception {
		Path copy = this.root.resolve(outsideFile.getRoot().relativize(outsideFile).toString());
		Files.createDirectories(copy.getParent());
		return copy;
	}

	/**
	 * Writes the instruction that makes a method reference into a functional
	 * interface.
	 */
	private static void reference(MethodVisitor mv, String method, String interfaceDescriptor, String erased,
			Handle target, String instantiated) {
		Handle metafactory = new Handle(Opcodes.H_INVOKESTATIC, "java/lang/invoke/LambdaMetafactory", "metafactory",
				"(Ljava/lang/invoke/MethodHandles$Lookup;Ljava/lang/String;Ljava/lang/invoke/MethodType;"
						+ "Ljava/lang/invoke/MethodType;Ljava/lang/invoke/MethodHandle;Ljava/lang/invoke/MethodType;)"
						+ "Ljava/lang/invoke/CallSite;",
				false);
		mv.visitInvokeDynamicInsn(method, interfaceDescriptor, metafactory, Type.getType(erased), target,
				Type.getType(instantiated));
	}

	private static void writeZip(Path file) throws Exception {
		try (OutputStream out = Files.newOutputStream(file); ZipOutputStream zip = new ZipOutputStream(out)) {
			zip.putNextEntry(new ZipEntry("entry.txt"));
			zip.write("zipped".getBytes(StandardCharsets.UTF_8));
			zip.closeEntry();
		}
	}

	/**
	 * A method body that opens a file by name with one of a class's constructors,
	 * giving the other arguments a usable value.
	 */
	private static Consumer<MethodVisitor> openByName(String owner, String descriptor, String path) {
		return mv -> {
			mv.visitTypeInsn(Opcodes.NEW, owner);
			mv.visitInsn(Opcodes.DUP);
			mv.visitLdcInsn(path);
			Type[] arguments = Type.getArgumentTypes(descriptor);
			for (int i = 1; i < arguments.length; i++) {
				switch (arguments[i].getDescriptor()) {
				case "Ljava/lang/String;" -> mv.visitLdcInsn(owner.endsWith("RandomAccessFile") ? "r" : "UTF-8");
				case "Ljava/nio/charset/Charset;" -> mv.visitFieldInsn(Opcodes.GETSTATIC,
						"java/nio/charset/StandardCharsets", "UTF_8", "Ljava/nio/charset/Charset;");
				case "Z" -> mv.visitInsn(Opcodes.ICONST_1);
				default -> mv.visitInsn(Opcodes.ACONST_NULL);
				}
			}
			mv.visitMethodInsn(Opcodes.INVOKESPECIAL, owner, "<init>", descriptor, false);
		};
	}

	private static Consumer<MethodVisitor> newFile(String path) {
		return mv -> {
			mv.visitTypeInsn(Opcodes.NEW, "java/io/File");
			mv.visitInsn(Opcodes.DUP);
			mv.visitLdcInsn(path);
			mv.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/io/File", "<init>", "(Ljava/lang/String;)V", false);
		};
	}

	private static Consumer<MethodVisitor> path(String location) {
		return mv -> {
			mv.visitLdcInsn(location);
			mv.visitInsn(Opcodes.ICONST_0);
			mv.visitTypeInsn(Opcodes.ANEWARRAY, "java/lang/String");
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/nio/file/Paths", "get",
					"(Ljava/lang/String;[Ljava/lang/String;)Ljava/nio/file/Path;", false);
		};
	}

	/**
	 * A method body that starts from the user's folder and walks out of it with
	 * {@code resolve}, leaving the path on the stack.
	 */
	private Consumer<MethodVisitor> escapingPath(String relative) {
		return mv -> {
			path(this.root.toString()).accept(mv);
			mv.visitLdcInsn(relative);
			mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, PATH, "resolve", "(Ljava/lang/String;)Ljava/nio/file/Path;",
					true);
		};
	}

	private Consumer<MethodVisitor> escapingFile(String relative) {
		return mv -> {
			escapingPath(relative).accept(mv);
			mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, PATH, "toFile", "()Ljava/io/File;", true);
		};
	}
}
