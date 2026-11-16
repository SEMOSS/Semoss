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

import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import prerna.engine.api.IEngine;
import prerna.engine.impl.pipeline.AdminEngineCalls;
import prerna.engine.impl.pipeline.PlatformOperation;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Builds a small class with ASM and loads it the way a project's custom
 * reactors load, so a sandbox test can make one call and see what the sandbox
 * does with it. Every method of the class is public, static and returns an
 * Object.
 */
public final class SandboxProbe {

	static final String INTERNAL_NAME = "custom/sandbox/RuleProbe";

	/**
	 * A class loader a test leaves for a probe to read, as a platform class might
	 * hand one out.
	 */
	public static ClassLoader heldLoader;

	/**
	 * An object a test leaves for a probe to read and cast.
	 */
	public static Object heldObject;

	private final ClassWriter writer = new ClassWriter(ClassWriter.COMPUTE_MAXS);

	/**
	 * Starts an empty probe class.
	 */
	SandboxProbe() {
		this(INTERNAL_NAME, "java/lang/Object");
	}

	/**
	 * Starts an empty class with its own name and superclass.
	 *
	 * @param internalName the class's internal name
	 * @param superName    the internal name of the class it extends
	 */
	SandboxProbe(String internalName, String superName) {
		this(internalName, superName, new String[0]);
	}

	/**
	 * Starts an empty class with its own name, superclass and interfaces.
	 *
	 * @param internalName the class's internal name
	 * @param superName    the internal name of the class it extends
	 * @param interfaces   the internal names of the interfaces it implements
	 */
	SandboxProbe(String internalName, String superName, String[] interfaces) {
		this.writer.visit(Opcodes.V21, Opcodes.ACC_PUBLIC | Opcodes.ACC_SUPER, internalName, null, superName,
				interfaces);
	}

	/**
	 * Puts an annotation with one enum setting on the class.
	 *
	 * @param descriptor     the annotation's descriptor
	 * @param setting        the setting's name
	 * @param enumDescriptor the descriptor of the setting's enum
	 * @param value          the enum constant
	 * @return this probe
	 */
	SandboxProbe annotation(String descriptor, String setting, String enumDescriptor, String value) {
		AnnotationVisitor annotation = this.writer.visitAnnotation(descriptor, true);
		annotation.visitEnum(setting, enumDescriptor, value);
		annotation.visitEnd();
		return this;
	}

	/**
	 * Adds a public constructor.
	 *
	 * @param descriptor the constructor's descriptor
	 * @param body       the instructions, starting with the call to the superclass
	 *                   constructor
	 * @return this probe
	 */
	SandboxProbe constructor(String descriptor, Consumer<MethodVisitor> body) {
		MethodVisitor mv = this.writer.visitMethod(Opcodes.ACC_PUBLIC, "<init>", descriptor, null, null);
		mv.visitCode();
		body.accept(mv);
		mv.visitInsn(Opcodes.RETURN);
		mv.visitMaxs(0, 0);
		mv.visitEnd();
		return this;
	}

	/**
	 * Adds a public instance method that returns an object.
	 *
	 * @param name       the method's name
	 * @param descriptor the method's descriptor
	 * @param body       the instructions; they leave the returned object on the
	 *                   stack
	 * @return this probe
	 */
	SandboxProbe instanceMethod(String name, String descriptor, Consumer<MethodVisitor> body) {
		MethodVisitor mv = this.writer.visitMethod(Opcodes.ACC_PUBLIC, name, descriptor, null, null);
		mv.visitCode();
		body.accept(mv);
		mv.visitInsn(Opcodes.ARETURN);
		mv.visitMaxs(0, 0);
		mv.visitEnd();
		return this;
	}

	/**
	 * Adds a method to the probe class.
	 *
	 * @param name the method's name
	 * @param body the instructions; they leave the returned Object on the stack
	 * @return this probe
	 */
	SandboxProbe method(String name, Consumer<MethodVisitor> body) {
		MethodVisitor mv = this.writer.visitMethod(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC, name,
				"()Ljava/lang/Object;", null, null);
		mv.visitCode();
		body.accept(mv);
		mv.visitInsn(Opcodes.ARETURN);
		mv.visitMaxs(0, 0);
		mv.visitEnd();
		return this;
	}

	/**
	 * The probe class as the compiler would have written it, before the sandbox
	 * touches it.
	 *
	 * @return the class file bytes
	 */
	byte[] bytes() {
		this.writer.visitEnd();
		return this.writer.toByteArray();
	}

	/**
	 * A method body that makes one call with null and zero arguments, on a null
	 * receiver when the call is not static, and returns what the call returned when
	 * that is an object.
	 *
	 * @param opcode     how the call is made, such as {@link Opcodes#INVOKESTATIC}
	 * @param owner      the internal name of the class the call names
	 * @param name       the method
	 * @param descriptor the method's descriptor
	 * @return the body
	 */
	static Consumer<MethodVisitor> call(int opcode, String owner, String name, String descriptor) {
		return mv -> {
			if (opcode != Opcodes.INVOKESTATIC) {
				mv.visitInsn(Opcodes.ACONST_NULL);
			}
			for (Type argument : Type.getArgumentTypes(descriptor)) {
				switch (argument.getSort()) {
				case Type.OBJECT, Type.ARRAY -> mv.visitInsn(Opcodes.ACONST_NULL);
				case Type.LONG -> mv.visitInsn(Opcodes.LCONST_0);
				case Type.FLOAT -> mv.visitInsn(Opcodes.FCONST_0);
				case Type.DOUBLE -> mv.visitInsn(Opcodes.DCONST_0);
				default -> mv.visitInsn(Opcodes.ICONST_0);
				}
			}
			mv.visitMethodInsn(opcode, owner, name, descriptor, opcode == Opcodes.INVOKEINTERFACE);
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
	}

	/**
	 * A sandboxed class whose {@code run} method makes one call, after checking
	 * that the call is in the class as compiled and gone once the sandbox has
	 * rewritten it.
	 *
	 * @param folder     the classes folder
	 * @param owner      the internal name of the class the call names
	 * @param name       the method
	 * @param descriptor the method's descriptor
	 * @param isStatic   whether the call is static
	 * @return the sandboxed class
	 * @throws Exception if the class cannot be written or loaded
	 */
	static Class<?> loadCall(Path folder, String owner, String name, String descriptor, boolean isStatic)
			throws Exception {
		int opcode = isStatic ? Opcodes.INVOKESTATIC : Opcodes.INVOKEVIRTUAL;
		byte[] original = new SandboxProbe().method("run", call(opcode, owner, name, descriptor)).bytes();
		String call = owner + "." + name;
		if (!calls(original).contains(call)) {
			throw new AssertionError("The probe does not call " + call);
		}
		if (calls(SemossClassLoader.applyBytecodeTransformations(original)).contains(call)) {
			throw new AssertionError("The sandbox left the call to " + call);
		}
		return load(folder, original);
	}

	/**
	 * Loads a probe class from a project's classes folder through the sandbox.
	 *
	 * @param folder the classes folder
	 * @param bytes  the class as compiled
	 * @return the sandboxed class
	 * @throws Exception if the class cannot be written or loaded
	 */
	static Class<?> load(Path folder, byte[] bytes) throws Exception {
		write(folder, INTERNAL_NAME, bytes);
		return loader(folder).loadClass(INTERNAL_NAME.replace('/', '.'));
	}

	/**
	 * Puts a compiled class in a project's classes folder.
	 *
	 * @param folder       the classes folder
	 * @param internalName the class's internal name
	 * @param bytes        the class as compiled
	 * @throws Exception if the class cannot be written
	 */
	static void write(Path folder, String internalName, byte[] bytes) throws Exception {
		Path classFile = folder.resolve(internalName + ".class");
		Files.createDirectories(classFile.getParent());
		Files.write(classFile, bytes);
	}

	/**
	 * A sandbox loader over a project's classes folder.
	 *
	 * @param folder the classes folder
	 * @return the loader
	 */
	static SemossClassLoader loader(Path folder) {
		return loader(folder, null);
	}

	/**
	 * A sandbox loader over the classes folder of one project.
	 *
	 * @param folder    the classes folder
	 * @param projectId the id of the project the classes belong to
	 * @return the loader
	 */
	static SemossClassLoader loader(Path folder, String projectId) {
		SemossClassLoader loader = new SemossClassLoader(SandboxProbe.class.getClassLoader(), projectId);
		loader.setFolder(folder.toString());
		return loader;
	}

	/**
	 * Loads a probe class as the custom code of one project.
	 *
	 * @param folder    the classes folder
	 * @param bytes     the class as compiled
	 * @param projectId the id of the project the class belongs to
	 * @return the sandboxed class
	 * @throws Exception if the class cannot be written or loaded
	 */
	static Class<?> load(Path folder, byte[] bytes, String projectId) throws Exception {
		write(folder, INTERNAL_NAME, bytes);
		return loader(folder, projectId).loadClass(INTERNAL_NAME.replace('/', '.'));
	}

	/**
	 * Runs a piece of work the way a platform method runs a callback it was
	 * handed, so the platform is between the caller and the work on the stack.
	 *
	 * @param work the callback
	 * @return what the callback returned
	 */
	public static Object runAsPlatform(Supplier<?> work) {
		return work.get();
	}

	/**
	 * Begins a platform operation and reads the held engine's name through a
	 * reactor of the platform, the way a platform method that syncs an engine
	 * would if a custom reactor reached it.
	 *
	 * @return the engine's name
	 */
	public static Object readHeldEngineInAPlatformOperation() {
		try (PlatformOperation operation = PlatformOperation.begin("sandbox probe")) {
			return new ReadsTheHeldEngine().execute().getValue();
		}
	}

	/**
	 * Reads the held engine's name through a reactor of the platform that a class
	 * requiring an admin runs, the way an admin reactor reads an engine.
	 *
	 * @return the engine's name
	 * @throws Throwable what the read threw
	 */
	public static Object readHeldEngineThroughAnAdminClass() throws Throwable {
		Object[] name = new Object[1];
		AdminEngineCalls.run(() -> name[0] = new ReadsTheHeldEngine().execute().getValue());
		return name[0];
	}

	/**
	 * Runs one method of a loaded probe class.
	 *
	 * @param probe  the sandboxed class
	 * @param method the method's name
	 * @return what the method returned
	 * @throws Throwable what the method threw
	 */
	static Object run(Class<?> probe, String method) throws Throwable {
		try {
			return probe.getMethod(method).invoke(null);
		} catch (InvocationTargetException e) {
			throw e.getCause();
		}
	}

	/**
	 * Every method a class calls, as {@code owner.name} with the owner's internal
	 * name.
	 *
	 * @param classBytes the class file
	 * @return the calls, in order
	 */
	static List<String> calls(byte[] classBytes) {
		List<String> calls = new ArrayList<>();
		new ClassReader(classBytes).accept(new ClassVisitor(Opcodes.ASM9) {
			@Override
			public MethodVisitor visitMethod(int access, String name, String descriptor, String signature,
					String[] exceptions) {
				return new MethodVisitor(Opcodes.ASM9) {
					@Override
					public void visitMethodInsn(int opcode, String owner, String called, String calledDescriptor,
							boolean isInterface) {
						calls.add(owner + "." + called);
					}
				};
			}
		}, 0);
		return calls;
	}

	/**
	 * A reactor of the platform that reads the held engine, as a tool a user runs
	 * does.
	 */
	private static final class ReadsTheHeldEngine extends AbstractReactor {

		@Override
		public NounMetadata execute() {
			return new NounMetadata(((IEngine) heldObject).getEngineName(), PixelDataType.CONST_STRING);
		}

	}

}
