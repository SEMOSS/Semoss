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

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLStreamHandlerFactory;
import java.nio.ByteBuffer;
import java.security.CodeSource;
import java.security.ProtectionDomain;

/**
 * Loads the classes in a project's own jars with the same sandbox as the
 * classes in its folder: every class the jar asks for is checked against
 * {@link SemossClassLoader#isReflectionApiBlocked(String)}, and every class
 * defined from the jar is rewritten by
 * {@link SemossClassLoader#applyBytecodeTransformations(byte[])} first. A jar
 * cannot define classes in {@code prerna.*} packages.
 *
 * <p>
 * It is also what a project's code gets when it creates a
 * {@code URLClassLoader}, and the class a custom {@code URLClassLoader}
 * subclass extends once the sandbox has rewritten it. For those, the parent is
 * always the sandbox loader of the project class that asked, and only jars and
 * folders on the local disk can be loaded.
 */
public class SemossProjectJarClassLoader extends URLClassLoader {

	/**
	 * A loader for a project's jars.
	 *
	 * @param urls   the jars
	 * @param parent the project's {@link SemossClassLoader}
	 */
	public SemossProjectJarClassLoader(URL[] urls, ClassLoader parent) {
		super(urls, parent);
	}

	/**
	 * A loader a project class created or extends, under that class's sandbox
	 * loader.
	 *
	 * @param urls  the local jars and folders
	 * @param owner the project class that created or extends this loader
	 */
	protected SemossProjectJarClassLoader(URL[] urls, Class<?> owner) {
		super(SandboxedClassLoading.sandboxedJarUrls(urls), SandboxedClassLoading.callerLoader(owner));
	}

	/**
	 * A loader a project class created or extends, under that class's sandbox
	 * loader.
	 *
	 * @param urls            the local jars and folders
	 * @param requestedParent the parent the project class asked for, which is not
	 *                        used
	 * @param owner           the project class that created or extends this loader
	 */
	protected SemossProjectJarClassLoader(URL[] urls, ClassLoader requestedParent, Class<?> owner) {
		this(urls, owner);
	}

	/**
	 * A loader a project class created or extends, under that class's sandbox
	 * loader.
	 *
	 * @param urls             the local jars and folders
	 * @param requestedParent  the parent the project class asked for, which is not
	 *                         used
	 * @param requestedFactory the URL handler factory the project class asked for,
	 *                         which is not used
	 * @param owner            the project class that created or extends this loader
	 */
	protected SemossProjectJarClassLoader(URL[] urls, ClassLoader requestedParent,
			URLStreamHandlerFactory requestedFactory, Class<?> owner) {
		this(urls, owner);
	}

	/**
	 * A named loader a project class created or extends, under that class's sandbox
	 * loader.
	 *
	 * @param name            the loader's name
	 * @param urls            the local jars and folders
	 * @param requestedParent the parent the project class asked for, which is not
	 *                        used
	 * @param owner           the project class that created or extends this loader
	 */
	protected SemossProjectJarClassLoader(String name, URL[] urls, ClassLoader requestedParent, Class<?> owner) {
		super(name, SandboxedClassLoading.sandboxedJarUrls(urls), SandboxedClassLoading.callerLoader(owner));
	}

	/**
	 * A named loader a project class created or extends, under that class's sandbox
	 * loader.
	 *
	 * @param name             the loader's name
	 * @param urls             the local jars and folders
	 * @param requestedParent  the parent the project class asked for, which is not
	 *                         used
	 * @param requestedFactory the URL handler factory the project class asked for,
	 *                         which is not used
	 * @param owner            the project class that created or extends this loader
	 */
	protected SemossProjectJarClassLoader(String name, URL[] urls, ClassLoader requestedParent,
			URLStreamHandlerFactory requestedFactory, Class<?> owner) {
		this(name, urls, requestedParent, owner);
	}

	/**
	 * Loads a class under the sandbox's block list, from the parent first and then
	 * from the jars.
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
	 * from the jars.
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
	 * Finds a class in the jars and rewrites it before defining it.
	 *
	 * @param name the class name
	 * @return the sandboxed class
	 * @throws ClassNotFoundException if the jars do not hold the class, or the
	 *                                sandbox refuses it
	 */
	@Override
	protected Class<?> findClass(String name) throws ClassNotFoundException {
		if (name.startsWith("prerna.")) {
			throw new ClassNotFoundException(
					"Classes in 'prerna.*' packages cannot be loaded from project reactor jars: " + name);
		}
		URL resource = findResource(name.replace('.', '/') + ".class");
		if (resource == null) {
			throw new ClassNotFoundException(name);
		}
		byte[] original;
		try (InputStream in = resource.openStream()) {
			original = in.readAllBytes();
		} catch (IOException e) {
			throw new ClassNotFoundException("Could not read class '" + name + "' from its jar", e);
		}
		byte[] sandboxed;
		try {
			sandboxed = SemossClassLoader.applyBytecodeTransformations(original);
		} catch (SecurityException e) {
			throw new ClassNotFoundException("Class '" + name + "' blocked by sandbox: " + e.getMessage(), e);
		} catch (IllegalArgumentException e) {
			throw new ClassNotFoundException("Class '" + name + "' cannot be sandbox-transformed: " + e.getMessage(),
					e);
		}
		return defineClass(name, sandboxed, 0, sandboxed.length);
	}

	/**
	 * Adds a local jar or folder a custom subclass asked for.
	 *
	 * @param url the jar or folder
	 */
	@Override
	protected void addURL(URL url) {
		super.addURL(SandboxedClassLoading.sandboxedJarUrl(url));
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
