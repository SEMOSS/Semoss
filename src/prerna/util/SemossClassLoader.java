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

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

/**
 * A custom class loader for dynamically loading project-specific classes in
 * Semoss. This class loader allows the application to load compiled Java
 * classes (.class files) from a specified folder that is not on the default
 * application classpath. This is essential for loading custom reactors and
 * other project-specific code at runtime.
 */
public class SemossClassLoader extends ClassLoader {

	private static final Logger classLogger = LogManager.getLogger(SemossClassLoader.class);

	private String folder = null;

	/**
	 * Constructs a new SemossClassLoader with a specified parent class loader.
	 *
	 * @param parent The parent class loader.
	 */
	public SemossClassLoader(ClassLoader parent) {
		super(parent);
	}

	/**
	 * Constructs a new SemossClassLoader and registers additional system properties
	 * that reactor code loaded by this instance is permitted to read.
	 *
	 * <p>
	 * Map semantics - for each entry:
	 * <ul>
	 * <li>Non-null value: reactor always receives this value for the key (platform
	 * override, decoupled from the real system property).</li>
	 * <li>Null value: the real {@code System.getProperty(key)} value is returned
	 * (allow-list the key, expose its actual value).</li>
	 * </ul>
	 *
	 * @param parent               the parent class loader
	 * @param additionalProperties extra properties to expose to reactor code;
	 *                             {@code null} is treated as an empty map
	 */
	public SemossClassLoader(ClassLoader parent, Map<String, String> additionalProperties) {
		super(parent);
		SandboxedJavaExecution.addPlatformProperties(additionalProperties);
	}

	/**
	 * Sets the base folder from which to load .class files.
	 * 
	 * @param folder The absolute path to the directory containing the compiled
	 *               classes.
	 */
	public void setFolder(String folder) {
		this.folder = folder;
	}

	/**
	 * Finds and loads the class from a .class file. This method is called when a
	 * class is not found in the parent class loader's path.
	 *
	 * @param name The fully qualified name of the class to load.
	 * @return The resulting Class object.
	 * @throws ClassNotFoundException If the class could not be found.
	 */
	private Class<?> getClass(String name) throws ClassNotFoundException {
		// We are getting a name that looks like
		// prerna.project.MyCustomClass
		// and we have to convert it into the .class file name
		// like prerna/project/MyCustomClass.class
		String file = name.replace('.', File.separatorChar) + ".class";
		byte[] b = null;
		try {
			// This loads the byte code data from the file
			b = loadClassData(file);
			// defineClass is inherited from the ClassLoader class
			// and converts the byte array into a Class
			if (b != null) {
				b = applyBytecodeTransformations(b);
				Class<?> c = defineClass(name, b, 0, b.length);
				resolveClass(c);
				return c;
			}
		} catch (IOException e) {
			classLogger.error("Failed to read class file for '{}' from folder '{}'", name, folder, e);
		} catch (SecurityException e) {
			throw new ClassNotFoundException("Class '" + name + "' blocked by sandbox: " + e.getMessage(), e);
		} catch (IllegalArgumentException e) {
			// ASM throws this when the class file was compiled with a newer Java version
			// than the ASM library supports ("Unsupported class file major version N").
			// Deny the load rather than allowing untransformed bytecode through.
			throw new ClassNotFoundException(
					"Class '" + name + "' cannot be sandbox-transformed (unsupported class file version"
							+ " - upgrade asm-commons in pom.xml): " + e.getMessage(),
					e);
		}
		return null;
	}

	/**
	 * Returns true if the given class name belongs to a JDK API that provides
	 * private reflective access to fields or objects. Custom project reactor code
	 * must not be able to use these APIs to bypass the SystemEngineRegistry access
	 * controls.
	 *
	 * Blocked: - java.lang.reflect.* direct Field/Method/Constructor reflection -
	 * java.lang.invoke.MethodHandles privateLookupIn() gives equivalent access -
	 * java.lang.invoke.MethodHandles$Lookup the lookup object itself -
	 * sun.misc.Unsafe objectFieldOffset+getObject reads any field -
	 * jdk.internal.misc.Unsafe same capability, internal variant
	 *
	 * @param name fully-qualified class name
	 * @return true if this class must not be loaded by project reactor code
	 */
	static boolean isReflectionApiBlocked(String name) {
		// Reflection / field access
		if (name.startsWith("java.lang.reflect.")) {
			return true;
		}
		if (name.equals("java.lang.invoke.MethodHandles")) {
			return true;
		}
		if (name.equals("java.lang.invoke.MethodHandles$Lookup")) {
			return true;
		}
		if (name.equals("sun.misc.Unsafe")) {
			return true;
		}
		if (name.equals("jdk.internal.misc.Unsafe")) {
			return true;
		}

		// Process execution
		if (name.equals("java.lang.ProcessBuilder")) {
			return true;
		}

		// Script engines - execute arbitrary code bypassing bytecode transformation
		if (name.startsWith("javax.script.")) {
			return true;
		}
		if (name.startsWith("jdk.nashorn.")) {
			return true;
		}
		if (name.startsWith("org.codehaus.groovy.")) {
			return true;
		}

		// Raw networking - data exfiltration / internal service access
		if (name.equals("java.net.Socket")) {
			return true;
		}
		if (name.equals("java.net.ServerSocket")) {
			return true;
		}
		if (name.equals("java.net.DatagramSocket")) {
			return true;
		}
		if (name.equals("java.net.MulticastSocket")) {
			return true;
		}
		if (name.startsWith("java.nio.channels.SocketChannel")) {
			return true;
		}
		if (name.startsWith("java.nio.channels.ServerSocketChannel")) {
			return true;
		}
		if (name.startsWith("java.nio.channels.DatagramChannel")) {
			return true;
		}

		// Thread creation - fork bombs, classloader replacement, context leaks
		if (name.equals("java.lang.Thread")) {
			return true;
		}
		if (name.startsWith("java.util.concurrent.ThreadPoolExecutor")) {
			return true;
		}
		if (name.startsWith("java.util.concurrent.ScheduledThreadPoolExecutor")) {
			return true;
		}
		if (name.startsWith("java.util.concurrent.ForkJoinPool")) {
			return true;
		}

		// Deserialization gadget chains
		if (name.equals("java.io.ObjectInputStream")) {
			return true;
		}

		// JVM diagnostics / classpath disclosure
		if (name.startsWith("java.lang.management.")) {
			return true;
		}

		return false;
	}

	/**
	 * Overrides the default class loading strategy. Blocks access to reflection
	 * APIs that could be used to bypass SystemEngineRegistry access controls, then
	 * attempts the parent class loader (standard delegation), and falls back to
	 * loading from the custom folder location if not found.
	 *
	 * @param name The fully qualified name of the class.
	 * @return The resulting Class object.
	 * @throws ClassNotFoundException If the class is blocked, or could not be found
	 *                                in either the parent loader or the custom
	 *                                path.
	 */
	@Override
	public Class<?> loadClass(String name) throws ClassNotFoundException {
		if (isReflectionApiBlocked(name)) {
			throw new ClassNotFoundException("Access to '" + name + "' is not permitted in project reactors");
		}
		Class retClass = null;
		// see if it is already loaded or in the classpath
		try {
			retClass = super.loadClass(name);
		} catch (Exception e) {
			// ignore
			// classLogger.error(Constants.STACKTRACE, e);
		}

		if (retClass == null) {
			if (name != null && name.startsWith("prerna.")) {
				classLogger.warn(
						"Project reactor requested prerna.* class '{}' - not found in application classpath, denying load from project folder",
						name);
				throw new ClassNotFoundException(
						"Classes in 'prerna.*' packages cannot be loaded from project reactor folders: " + name);
			}
			classLogger.info("Project Specific Class " + name);
			retClass = getClass(name);
		}
		return retClass;
	}

	/**
	 * Reads the raw bytecode of a class file from the custom folder location.
	 * 
	 * TODO: Need to incorporate loading jars. Not right now
	 *
	 * @param name The relative path of the .class file to load.
	 * @return A byte array containing the class data.
	 * @throws IOException If there is a problem reading the file.
	 */
	private byte[] loadClassData(String name) throws IOException {
		FileInputStream stream = null;
		DataInputStream in = null;
		byte buff[] = null;
		try {
			stream = new FileInputStream(new File(folder + "/" + name));
			int size = stream.available();
			buff = new byte[size];
			in = new DataInputStream(stream);
			// Reading the binary data
			in.readFully(buff);
		} finally {
			try {
				if (stream != null) {
					stream.close();
				}
			} catch (IOException e) {
				classLogger.error("Failed to close FileInputStream for class file '{}' in folder '{}'", name, folder,
						e);
			}
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException e) {
				classLogger.error("Failed to close DataInputStream for class file '{}' in folder '{}'", name, folder,
						e);
			}
		}
		return buff;
	}

	/**
	 * Finds the specified class. This method is overridden from ClassLoader. In
	 * this implementation, it delegates to the parent's findClass method.
	 *
	 * @param name The fully qualified name of the class.
	 * @return The resulting Class object.
	 * @throws ClassNotFoundException If the class could not be found.
	 */
	@Override
	protected Class findClass(String name) throws ClassNotFoundException {
		return super.findClass(name);
	}

	// =========================================================================
	// Bytecode transformation
	// =========================================================================

	/**
	 * Internal name of the sandbox utility class whose static factory methods
	 * replace direct file-API calls in the transformed bytecode.
	 */
	private static final String SANDBOX = "prerna/util/SandboxedJavaExecution";

	/**
	 * Maps each intercepted internal class name to its constructor descriptor(s),
	 * and for each descriptor the replacement factory method name and descriptor in
	 * {@link SandboxedPaths}.
	 *
	 * <p>
	 * Layout:
	 * {@code classInternalName -> constructorDescriptor -> [factoryName, factoryDescriptor]}
	 *
	 * <p>
	 * Every constructor overload of each type must be present so that a suppressed
	 * {@code NEW} always finds a matching factory entry. Constructors that carry no
	 * path information (e.g. {@code FileDescriptor}-based ones) are mapped to
	 * passthrough factories.
	 */
	private static final Map<String, Map<String, String[]>> CONSTRUCTOR_REDIRECTS;

	static {
		Map<String, Map<String, String[]>> m = new HashMap<>();

		// java.io.File - all four public constructors
		Map<String, String[]> file = new HashMap<>();
		file.put("(Ljava/lang/String;)V", new String[] { "file", "(Ljava/lang/String;)Ljava/io/File;" });
		file.put("(Ljava/lang/String;Ljava/lang/String;)V",
				new String[] { "file", "(Ljava/lang/String;Ljava/lang/String;)Ljava/io/File;" });
		file.put("(Ljava/io/File;Ljava/lang/String;)V",
				new String[] { "file", "(Ljava/io/File;Ljava/lang/String;)Ljava/io/File;" });
		file.put("(Ljava/net/URI;)V", new String[] { "file", "(Ljava/net/URI;)Ljava/io/File;" });
		m.put("java/io/File", file);

		// java.io.FileInputStream - all three public constructors
		Map<String, String[]> fis = new HashMap<>();
		fis.put("(Ljava/lang/String;)V",
				new String[] { "fileInputStream", "(Ljava/lang/String;)Ljava/io/FileInputStream;" });
		fis.put("(Ljava/io/File;)V", new String[] { "fileInputStream", "(Ljava/io/File;)Ljava/io/FileInputStream;" });
		fis.put("(Ljava/io/FileDescriptor;)V",
				new String[] { "fileInputStream", "(Ljava/io/FileDescriptor;)Ljava/io/FileInputStream;" });
		m.put("java/io/FileInputStream", fis);

		// java.io.FileOutputStream - all five public constructors
		Map<String, String[]> fos = new HashMap<>();
		fos.put("(Ljava/lang/String;)V",
				new String[] { "fileOutputStream", "(Ljava/lang/String;)Ljava/io/FileOutputStream;" });
		fos.put("(Ljava/lang/String;Z)V",
				new String[] { "fileOutputStream", "(Ljava/lang/String;Z)Ljava/io/FileOutputStream;" });
		fos.put("(Ljava/io/File;)V", new String[] { "fileOutputStream", "(Ljava/io/File;)Ljava/io/FileOutputStream;" });
		fos.put("(Ljava/io/File;Z)V",
				new String[] { "fileOutputStream", "(Ljava/io/File;Z)Ljava/io/FileOutputStream;" });
		fos.put("(Ljava/io/FileDescriptor;)V",
				new String[] { "fileOutputStream", "(Ljava/io/FileDescriptor;)Ljava/io/FileOutputStream;" });
		m.put("java/io/FileOutputStream", fos);

		CONSTRUCTOR_REDIRECTS = Collections.unmodifiableMap(m);
	}

	/**
	 * Applies all bytecode transformations to a raw class byte array:
	 * <ul>
	 * <li>Redirects {@code new File(...)}, {@code new FileInputStream(String)}, and
	 * {@code new FileOutputStream(String[,boolean])} through {@link SandboxedPaths}
	 * factory methods that enforce the sandbox root.</li>
	 * <li>Redirects {@code Paths.get(...)} and {@code Path.of(...)} through
	 * sandboxed equivalents.</li>
	 * <li>Replaces {@code System.getenv()} / {@code System.getenv(String)} with
	 * {@code null}, hiding host environment variables.</li>
	 * <li>Replaces all {@code Runtime.exec(...)} overloads with a call that throws
	 * {@link SecurityException}.</li>
	 * </ul>
	 *
	 * @param original raw bytecode of the class to transform
	 * @return transformed bytecode
	 */
	static byte[] applyBytecodeTransformations(byte[] original) {
		ClassReader cr = new ClassReader(original);
		// COMPUTE_FRAMES rebuilds stack-map frames from scratch, which is required
		// because our instruction replacements can change the frame layout.
		// getCommonSuperClass falls back to Object for any class not on the
		// current classpath so that transformation never fails hard.
		ClassWriter cw = new ClassWriter(cr, ClassWriter.COMPUTE_FRAMES) {
			@Override
			protected String getCommonSuperClass(String type1, String type2) {
				try {
					return super.getCommonSuperClass(type1, type2);
				} catch (Exception e) {
					return "java/lang/Object";
				}
			}
		};
		// SKIP_FRAMES: let COMPUTE_FRAMES recompute everything instead of trying
		// to patch the original (potentially stale) frame data.
		cr.accept(new SandboxClassVisitor(cw), ClassReader.SKIP_FRAMES);
		return cw.toByteArray();
	}

	// -------------------------------------------------------------------------
	// ASM visitor - class level
	// -------------------------------------------------------------------------

	private static final class SandboxClassVisitor extends ClassVisitor {

		SandboxClassVisitor(ClassVisitor cv) {
			super(Opcodes.ASM9, cv);
		}

		@Override
		public MethodVisitor visitMethod(int access, String name, String descriptor, String signature,
				String[] exceptions) {
			MethodVisitor mv = super.visitMethod(access, name, descriptor, signature, exceptions);
			return new SandboxMethodVisitor(mv);
		}
	}

	// -------------------------------------------------------------------------
	// ASM visitor - method level
	// -------------------------------------------------------------------------

	/**
	 * Rewrites individual bytecode instructions inside each method body.
	 *
	 * <h3>Constructor redirect strategy</h3>
	 * <p>
	 * Java compilers always emit
	 * {@code NEW T; DUP; [push args]; INVOKESPECIAL T.<init>}. We suppress the
	 * {@code NEW} and its companion {@code DUP}, then at {@code INVOKESPECIAL} time
	 * we emit {@code INVOKESTATIC SandboxedPaths.factory(args)} instead. The
	 * factory returns an already-initialized instance, leaving the stack in exactly
	 * the same state as the original sequence would have.
	 *
	 * <p>
	 * Nesting (e.g. {@code new FileInputStream(new File("x"))}) is handled
	 * correctly by maintaining a LIFO stack of suppressed {@code NEW} types: the
	 * innermost {@code INVOKESPECIAL} always matches the innermost suppressed
	 * {@code NEW}.
	 */
	private static final class SandboxMethodVisitor extends MethodVisitor {

		/**
		 * LIFO stack of internal class names for {@code NEW} instructions we have
		 * suppressed. Each entry is waiting for its matching
		 * {@code INVOKESPECIAL <init>}.
		 */
		private final Deque<String> pendingNews = new ArrayDeque<>();

		/**
		 * Set to {@code true} immediately after suppressing a {@code NEW} so that the
		 * very next {@code DUP} instruction (which always immediately follows
		 * {@code NEW} in compiler-generated bytecode) is also suppressed.
		 */
		private boolean suppressNextDup = false;

		SandboxMethodVisitor(MethodVisitor mv) {
			super(Opcodes.ASM9, mv);
		}

		@Override
		public void visitTypeInsn(int opcode, String type) {
			if (opcode == Opcodes.NEW && CONSTRUCTOR_REDIRECTS.containsKey(type)) {
				pendingNews.push(type);
				suppressNextDup = true;
				// Do NOT emit the NEW - factory method replaces the whole sequence.
			} else {
				suppressNextDup = false;
				super.visitTypeInsn(opcode, type);
			}
		}

		@Override
		public void visitInsn(int opcode) {
			// Suppress the DUP that always immediately follows a suppressed NEW.
			if (opcode == Opcodes.DUP && suppressNextDup) {
				suppressNextDup = false;
				return;
			}
			suppressNextDup = false;
			super.visitInsn(opcode);
		}

		@Override
		public void visitMethodInsn(int opcode, String owner, String name, String descriptor, boolean itf) {
			suppressNextDup = false;

			// ------------------------------------------------------------------
			// Constructor redirect: replace NEW+DUP+INVOKESPECIAL with a static
			// factory call whose return type is the constructed object.
			// ------------------------------------------------------------------
			if (opcode == Opcodes.INVOKESPECIAL && "<init>".equals(name) && !pendingNews.isEmpty()
					&& owner.equals(pendingNews.peek())) {
				pendingNews.pop();
				Map<String, String[]> ctorMap = CONSTRUCTOR_REDIRECTS.get(owner);
				String[] factory = ctorMap != null ? ctorMap.get(descriptor) : null;
				if (factory != null) {
					// factory[0] = method name, factory[1] = method descriptor
					super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, factory[0], factory[1], false);
				} else {
					// Unknown overload for a sandboxed type - pop args and block.
					for (Type argType : Type.getArgumentTypes(descriptor)) {
						super.visitInsn(argType.getSize() == 2 ? Opcodes.POP2 : Opcodes.POP);
					}
					super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "blockUnknownConstructor", "()V", false);
					super.visitInsn(Opcodes.ACONST_NULL); // unreachable; satisfies verifier
				}
				return;
			}

			// ------------------------------------------------------------------
			// Block System.exit / Runtime.halt - would kill the entire JVM.
			// ------------------------------------------------------------------
			if (opcode == Opcodes.INVOKESTATIC && "java/lang/System".equals(owner) && "exit".equals(name)) {
				super.visitInsn(Opcodes.POP); // pop the int status arg
				super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "blockExit", "()V", false);
				return;
			}
			if (opcode == Opcodes.INVOKEVIRTUAL && "java/lang/Runtime".equals(owner) && "halt".equals(name)) {
				super.visitInsn(Opcodes.POP); // pop the int status arg
				super.visitInsn(Opcodes.POP); // pop the Runtime instance
				super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "blockExit", "()V", false);
				return;
			}

			// ------------------------------------------------------------------
			// Sandbox Class.forName - the single-arg overload is safe because it
			// uses the calling class's classloader (SemossClassLoader), which
			// already enforces isReflectionApiBlocked. We still route it through
			// sandboxedForName so the block-list is checked before the load.
			//
			// The three-arg overload (String, boolean, ClassLoader) is dangerous:
			// the caller could pass ClassLoader.getSystemClassLoader() to bypass
			// SemossClassLoader entirely. We strip the extra args and fall back to
			// the sandboxed single-arg version.
			// ------------------------------------------------------------------
			if (opcode == Opcodes.INVOKESTATIC && "java/lang/Class".equals(owner) && "forName".equals(name)) {
				if ("(Ljava/lang/String;ZLjava/lang/ClassLoader;)Ljava/lang/Class;".equals(descriptor)) {
					// Stack: [..., className(String), initialize(boolean), loader(ClassLoader)]
					super.visitInsn(Opcodes.POP); // pop the ClassLoader
					super.visitInsn(Opcodes.POP); // pop the boolean (initialize)
					// className is now on top - fall through to sandboxedForName
				}
				// Stack: [..., className(String)]
				super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "sandboxedForName",
						"(Ljava/lang/String;)Ljava/lang/Class;", false);
				return;
			}

			// ------------------------------------------------------------------
			// Block System.getenv - return null instead of exposing env vars.
			// ------------------------------------------------------------------
			if (opcode == Opcodes.INVOKESTATIC && "java/lang/System".equals(owner) && "getenv".equals(name)) {
				if ("(Ljava/lang/String;)Ljava/lang/String;".equals(descriptor)) {
					super.visitInsn(Opcodes.POP); // discard the String key argument
				}
				// Both overloads return a reference - push null.
				super.visitInsn(Opcodes.ACONST_NULL);
				return;
			}

			// ------------------------------------------------------------------
			// Sandbox System.getProperty - route through SandboxedPaths which
			// allows the default safe set + any platform-registered properties,
			// and blocks everything else.
			// ------------------------------------------------------------------
			if (opcode == Opcodes.INVOKESTATIC && "java/lang/System".equals(owner) && "getProperty".equals(name)) {
				if ("(Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;".equals(descriptor)) {
					super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "sandboxedGetProperty",
							"(Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;", false);
				} else {
					super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "sandboxedGetProperty",
							"(Ljava/lang/String;)Ljava/lang/String;", false);
				}
				return;
			}
			if (opcode == Opcodes.INVOKESTATIC && "java/lang/System".equals(owner) && "getProperties".equals(name)) {
				super.visitInsn(Opcodes.ACONST_NULL);
				return;
			}

			// ------------------------------------------------------------------
			// Block System.setProperty / clearProperty / setProperties -
			// would affect JVM-wide state for all users.
			// ------------------------------------------------------------------
			if (opcode == Opcodes.INVOKESTATIC && "java/lang/System".equals(owner)
					&& ("setProperty".equals(name) || "clearProperty".equals(name) || "setProperties".equals(name))) {
				for (Type argType : Type.getArgumentTypes(descriptor)) {
					super.visitInsn(argType.getSize() == 2 ? Opcodes.POP2 : Opcodes.POP);
				}
				super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "blockSystemMutation", "()V", false);
				// setProperty returns String, clearProperty returns String, setProperties is
				// void
				if (!descriptor.endsWith(")V")) {
					super.visitInsn(Opcodes.ACONST_NULL);
				}
				return;
			}

			// ------------------------------------------------------------------
			// Block Thread.getAllStackTraces - reveals stack traces of all
			// threads across all users (information disclosure).
			// ------------------------------------------------------------------
			if (opcode == Opcodes.INVOKESTATIC && "java/lang/Thread".equals(owner)
					&& "getAllStackTraces".equals(name)) {
				super.visitInsn(Opcodes.ACONST_NULL);
				return;
			}

			// ------------------------------------------------------------------
			// Block Files.createSymbolicLink - reactor code could create a
			// symlink inside the chroot pointing to a path outside it, then
			// read through it via the admin-symlink path in sanitize().
			// ------------------------------------------------------------------
			if (opcode == Opcodes.INVOKESTATIC && "java/nio/file/Files".equals(owner)
					&& "createSymbolicLink".equals(name)) {
				for (Type argType : Type.getArgumentTypes(descriptor)) {
					super.visitInsn(argType.getSize() == 2 ? Opcodes.POP2 : Opcodes.POP);
				}
				super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "blockSymlinkCreation", "()V", false);
				super.visitInsn(Opcodes.ACONST_NULL); // fake Path return (unreachable)
				return;
			}

			// ------------------------------------------------------------------
			// Block FileChannel.open - NIO channel bypass for file access.
			// ------------------------------------------------------------------
			if (opcode == Opcodes.INVOKESTATIC && "java/nio/channels/FileChannel".equals(owner)
					&& "open".equals(name)) {
				for (Type argType : Type.getArgumentTypes(descriptor)) {
					super.visitInsn(argType.getSize() == 2 ? Opcodes.POP2 : Opcodes.POP);
				}
				super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "blockFileChannel", "()V", false);
				super.visitInsn(Opcodes.ACONST_NULL); // fake FileChannel return (unreachable)
				return;
			}

			// ------------------------------------------------------------------
			// Block URL.openStream / URL.openConnection - HTTP calls to
			// arbitrary external endpoints (data exfiltration).
			// ------------------------------------------------------------------
			if (opcode == Opcodes.INVOKEVIRTUAL && "java/net/URL".equals(owner)
					&& ("openStream".equals(name) || "openConnection".equals(name))) {
				super.visitInsn(Opcodes.POP); // pop the URL instance
				super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "blockNetworkAccess", "()V", false);
				super.visitInsn(Opcodes.ACONST_NULL); // fake return (unreachable)
				return;
			}

			// ------------------------------------------------------------------
			// Redirect Paths.get(String, String...) through the sandbox.
			// ------------------------------------------------------------------
			if (opcode == Opcodes.INVOKESTATIC && "java/nio/file/Paths".equals(owner) && "get".equals(name)
					&& "(Ljava/lang/String;[Ljava/lang/String;)Ljava/nio/file/Path;".equals(descriptor)) {
				super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "pathsGet",
						"(Ljava/lang/String;[Ljava/lang/String;)Ljava/nio/file/Path;", false);
				return;
			}

			// ------------------------------------------------------------------
			// Redirect Path.of(String, String...) through the sandbox.
			// ------------------------------------------------------------------
			if (opcode == Opcodes.INVOKESTATIC && "java/nio/file/Path".equals(owner) && "of".equals(name)
					&& "(Ljava/lang/String;[Ljava/lang/String;)Ljava/nio/file/Path;".equals(descriptor)) {
				super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "pathOf",
						"(Ljava/lang/String;[Ljava/lang/String;)Ljava/nio/file/Path;", false);
				return;
			}

			// ------------------------------------------------------------------
			// Sandbox File.getParentFile / File.getParent - return null if the
			// parent would escape the chroot, matching natural JVM behaviour at
			// the filesystem root.
			// ------------------------------------------------------------------
			if (opcode == Opcodes.INVOKEVIRTUAL && "java/io/File".equals(owner)) {
				if ("getParentFile".equals(name)) {
					super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "sandboxedGetParentFile",
							"(Ljava/io/File;)Ljava/io/File;", false);
					return;
				}
				if ("getParent".equals(name)) {
					super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "sandboxedGetParent",
							"(Ljava/io/File;)Ljava/lang/String;", false);
					return;
				}
				if ("getAbsolutePath".equals(name)) {
					super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "sandboxedGetAbsolutePath",
							"(Ljava/io/File;)Ljava/lang/String;", false);
					return;
				}
				if ("getCanonicalPath".equals(name)) {
					super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "sandboxedGetCanonicalPath",
							"(Ljava/io/File;)Ljava/lang/String;", false);
					return;
				}
				if ("toPath".equals(name)) {
					super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "sandboxedFileToPath",
							"(Ljava/io/File;)Ljava/nio/file/Path;", false);
					return;
				}
			}

			// ------------------------------------------------------------------
			// Block Runtime.exec - pop all args + instance, then throw.
			// ------------------------------------------------------------------
			if (opcode == Opcodes.INVOKEVIRTUAL && "java/lang/Runtime".equals(owner) && "exec".equals(name)) {
				// Pop constructor args right-to-left (top of stack first).
				for (Type argType : Type.getArgumentTypes(descriptor)) {
					super.visitInsn(argType.getSize() == 2 ? Opcodes.POP2 : Opcodes.POP);
				}
				super.visitInsn(Opcodes.POP); // pop the Runtime instance
				super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "blockRuntimeExec", "()V", false);
				super.visitInsn(Opcodes.ACONST_NULL); // fake Process return (unreachable)
				return;
			}

			super.visitMethodInsn(opcode, owner, name, descriptor, itf);
		}
	}

}
