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

import java.nio.ByteBuffer;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.security.SecureClassLoader;

/**
 * The class a project's own class loader extends once the sandbox has rewritten
 * it. A custom class written as {@code extends ClassLoader} or
 * {@code extends SecureClassLoader} is loaded as a subclass of this one, so the
 * classes it supplies stay in the sandbox:
 *
 * <ul>
 * <li>Its parent is always the sandbox loader of the project class that
 * declares it, whatever parent its constructor passes.</li>
 * <li>Every class it is asked for is checked against
 * {@link SemossClassLoader#isReflectionApiBlocked(String)} and looked for in
 * the parent first. The custom loader supplies its own classes through
 * {@code findClass}.</li>
 * <li>Every class it defines is rewritten by
 * {@link SemossClassLoader#applyBytecodeTransformations(byte[])} first.</li>
 * </ul>
 */
public class SandboxedClassLoader extends SecureClassLoader {

	/**
	 * A loader under the sandbox loader of the class that declares it.
	 *
	 * @param owner the project class that extends this loader
	 */
	protected SandboxedClassLoader(Class<?> owner) {
		super(SandboxedClassLoading.callerLoader(owner));
	}

	/**
	 * A loader under the sandbox loader of the class that declares it.
	 *
	 * @param requestedParent the parent the project class asked for, which is not
	 *                        used
	 * @param owner           the project class that extends this loader
	 */
	protected SandboxedClassLoader(ClassLoader requestedParent, Class<?> owner) {
		super(SandboxedClassLoading.callerLoader(owner));
	}

	/**
	 * A named loader under the sandbox loader of the class that declares it.
	 *
	 * @param name            the loader's name
	 * @param requestedParent the parent the project class asked for, which is not
	 *                        used
	 * @param owner           the project class that extends this loader
	 */
	protected SandboxedClassLoader(String name, ClassLoader requestedParent, Class<?> owner) {
		super(name, SandboxedClassLoading.callerLoader(owner));
	}

	/**
	 * Loads a class under the sandbox's block list, from the parent first and then
	 * from {@code findClass}.
	 *
	 * @param name the class name
	 * @return the class
	 * @throws ClassNotFoundException if the class is blocked or cannot be found
	 */
	@Override
	public final Class<?> loadClass(String name) throws ClassNotFoundException {
		return loadClass(name, false);
	}

	/**
	 * Loads a class under the sandbox's block list, from the parent first and then
	 * from {@code findClass}.
	 *
	 * @param name    the class name
	 * @param resolve whether to link the class
	 * @return the class
	 * @throws ClassNotFoundException if the class is blocked or cannot be found
	 */
	@Override
	protected final Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
		if (SemossClassLoader.isReflectionApiBlocked(name)) {
			throw new ClassNotFoundException("Access to '" + name + "' is not permitted in project reactors");
		}
		return super.loadClass(name, resolve);
	}

	/**
	 * Stands in for {@code findSystemClass}: finds the class through the sandbox.
	 *
	 * @param name the class name
	 * @return the class
	 * @throws ClassNotFoundException if the class is blocked or cannot be found
	 */
	protected final Class<?> findSandboxedSystemClass(String name) throws ClassNotFoundException {
		return loadClass(name, false);
	}

	/**
	 * Stands in for {@code defineClass(String, byte[], int, int)}: defines the
	 * class after the sandbox has rewritten it.
	 *
	 * @param name   the class name, or null to take it from the bytes
	 * @param bytes  the class file
	 * @param offset where the class file starts
	 * @param length how long the class file is
	 * @return the sandboxed class
	 */
	protected final Class<?> defineSandboxedClass(String name, byte[] bytes, int offset, int length) {
		byte[] sandboxed = SemossClassLoader.sandboxedDefinition(bytes, offset, length);
		return defineClass(name, sandboxed, 0, sandboxed.length);
	}

	/**
	 * Stands in for {@code defineClass(byte[], int, int)}: defines the class after
	 * the sandbox has rewritten it.
	 *
	 * @param bytes  the class file
	 * @param offset where the class file starts
	 * @param length how long the class file is
	 * @return the sandboxed class
	 */
	protected final Class<?> defineSandboxedClass(byte[] bytes, int offset, int length) {
		return defineSandboxedClass(null, bytes, offset, length);
	}

	/**
	 * Stands in for
	 * {@code defineClass(String, byte[], int, int, ProtectionDomain)}: defines the
	 * class after the sandbox has rewritten it.
	 *
	 * @param name   the class name, or null to take it from the bytes
	 * @param bytes  the class file
	 * @param offset where the class file starts
	 * @param length how long the class file is
	 * @param domain the class's protection domain
	 * @return the sandboxed class
	 */
	protected final Class<?> defineSandboxedClass(String name, byte[] bytes, int offset, int length,
			ProtectionDomain domain) {
		byte[] sandboxed = SemossClassLoader.sandboxedDefinition(bytes, offset, length);
		return defineClass(name, sandboxed, 0, sandboxed.length, domain);
	}

	/**
	 * Stands in for {@code defineClass(String, ByteBuffer, ProtectionDomain)}:
	 * defines the class after the sandbox has rewritten it.
	 *
	 * @param name   the class name, or null to take it from the bytes
	 * @param bytes  the class file
	 * @param domain the class's protection domain
	 * @return the sandboxed class
	 */
	protected final Class<?> defineSandboxedClass(String name, ByteBuffer bytes, ProtectionDomain domain) {
		byte[] sandboxed = SemossClassLoader.sandboxedDefinition(bytes);
		return defineClass(name, sandboxed, 0, sandboxed.length, domain);
	}

	/**
	 * Stands in for {@code defineClass(String, byte[], int, int, CodeSource)}:
	 * defines the class after the sandbox has rewritten it.
	 *
	 * @param name   the class name, or null to take it from the bytes
	 * @param bytes  the class file
	 * @param offset where the class file starts
	 * @param length how long the class file is
	 * @param source where the class came from
	 * @return the sandboxed class
	 */
	protected final Class<?> defineSandboxedClass(String name, byte[] bytes, int offset, int length,
			CodeSource source) {
		byte[] sandboxed = SemossClassLoader.sandboxedDefinition(bytes, offset, length);
		return defineClass(name, sandboxed, 0, sandboxed.length, source);
	}

	/**
	 * Stands in for {@code defineClass(String, ByteBuffer, CodeSource)}: defines
	 * the class after the sandbox has rewritten it.
	 *
	 * @param name   the class name, or null to take it from the bytes
	 * @param bytes  the class file
	 * @param source where the class came from
	 * @return the sandboxed class
	 */
	protected final Class<?> defineSandboxedClass(String name, ByteBuffer bytes, CodeSource source) {
		byte[] sandboxed = SemossClassLoader.sandboxedDefinition(bytes);
		return defineClass(name, sandboxed, 0, sandboxed.length, source);
	}
}
