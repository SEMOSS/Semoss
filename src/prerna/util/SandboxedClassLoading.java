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

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLStreamHandlerFactory;
import java.nio.file.Path;
import java.util.Set;

/**
 * The calls the sandbox writes into a project's classes in place of class
 * loading: finding a class by name, handing out a class loader, and creating a
 * loader for local jars. Every loader a project class gets is a sandbox loader
 * of its own project, and every class it loads goes through the block list.
 * The class also tells the platform which project, if any, a class belongs to.
 *
 * <p>
 * The helpers take the project class that asked as an argument, which only the
 * sandbox writes, so no project class can name or extend this class.
 */
public final class SandboxedClassLoading {

	private static final StackWalker STACK = StackWalker.getInstance(
			Set.of(StackWalker.Option.RETAIN_CLASS_REFERENCE, StackWalker.Option.SHOW_HIDDEN_FRAMES));

	private SandboxedClassLoading() {
	}

	/**
	 * Sandboxed replacement for {@code Class.forName}: finds the class through the
	 * sandbox loader of the project class that asked, so the block list applies and
	 * the project's own classes are found. The three-argument form is reduced to
	 * this by the bytecode transformer, so a loader the caller passes in is not
	 * used.
	 *
	 * @param className the class name
	 * @param caller    the project class that asked
	 * @return the class
	 * @throws SecurityException      if the class is on the reactor block list
	 * @throws ClassNotFoundException if the class cannot be found
	 */
	public static Class<?> sandboxedForName(String className, Class<?> caller) throws ClassNotFoundException {
		if (SemossClassLoader.isReflectionApiBlocked(className)) {
			throw new SecurityException("Class.forName('" + className + "') is not permitted in project reactors");
		}
		return Class.forName(className, true, callerLoader(caller));
	}

	/**
	 * Sandboxed replacement for {@code ClassLoader.loadClass(String)} on a loader a
	 * project class holds: applies the block list whichever loader it is.
	 *
	 * @param loader    the loader the project class called
	 * @param className the class name
	 * @return the class
	 * @throws SecurityException      if the class is on the reactor block list
	 * @throws ClassNotFoundException if the class cannot be found
	 */
	public static Class<?> sandboxedLoadClass(ClassLoader loader, String className) throws ClassNotFoundException {
		if (SemossClassLoader.isReflectionApiBlocked(className)) {
			throw new SecurityException("Loading '" + className + "' is not permitted in project reactors");
		}
		return loader.loadClass(className);
	}

	/**
	 * Sandboxed replacement for {@code Class.newInstance()}: creates the object
	 * when project reactor code could create it by name, and refuses a class on the
	 * block list and a platform class that works on a system database.
	 *
	 * @param type the class to create an object of
	 * @return the new object
	 * @throws SecurityException            if project reactor code cannot create
	 *                                      objects of the class
	 * @throws ReflectiveOperationException if the class has no public constructor
	 *                                      without arguments, or it fails
	 */
	public static Object sandboxedNewInstance(Class<?> type) throws ReflectiveOperationException {
		String name = type.getName();
		if (SemossClassLoader.isReflectionApiBlocked(name)
				|| SemossClassLoader.isSystemDatabaseClass(name.replace('.', '/'))) {
			throw new SecurityException("Creating " + name + " is not permitted in project reactors");
		}
		try {
			return type.getConstructor().newInstance();
		} catch (InvocationTargetException e) {
			throw new InstantiationException("Could not create " + name + ": " + e.getCause());
		}
	}

	/**
	 * True for a class a project's sandbox loaded: its custom reactors, the classes
	 * in its jars and its copies of libraries.
	 *
	 * @param candidate a class, or null
	 * @return true if the class is project code
	 */
	public static boolean isProjectClass(Class<?> candidate) {
		return candidate != null && SemossClassLoader.isSandboxLoader(candidate.getClassLoader());
	}

	/**
	 * Whether a project's custom code is acting on this thread: one of its classes
	 * is on the call stack, whether it makes the current call itself or the
	 * platform makes it on its behalf.
	 *
	 * @return true when a class a project's sandbox loaded is on the call stack
	 */
	public static boolean projectCodeIsOnTheStack() {
		return STACK.walk(frames -> frames.anyMatch(frame -> isProjectClass(frame.getDeclaringClass())));
	}

	/**
	 * The project a class belongs to: the project of the sandbox loader that loaded
	 * it, or of the nearest sandbox loader above that one, which covers the loaders
	 * a project's own code creates.
	 *
	 * @param candidate a class, or null
	 * @return the project id, or null when the class is not project code or its
	 *         loader belongs to no project
	 */
	public static String projectIdOf(Class<?> candidate) {
		ClassLoader loader = candidate == null ? null : candidate.getClassLoader();
		while (SemossClassLoader.isSandboxLoader(loader)) {
			String projectId = null;
			if (loader instanceof SemossClassLoader projectLoader) {
				projectId = projectLoader.getProjectId();
			} else if (loader instanceof SemossJarClassLoader dependencyLoader) {
				projectId = dependencyLoader.getProjectId();
			}
			if (projectId != null) {
				return projectId;
			}
			if (loader != null) {
				loader = loader.getParent();
			}
		}
		return null;
	}

	/**
	 * The sandbox loader that loaded a project class.
	 *
	 * @param caller the project class
	 * @return its loader
	 * @throws SecurityException if the class was not loaded by the sandbox
	 */
	static ClassLoader callerLoader(Class<?> caller) {
		ClassLoader loader = caller.getClassLoader();
		if (!SemossClassLoader.isSandboxLoader(loader)) {
			throw new SecurityException("Class loaders are only available to classes the project sandbox loaded");
		}
		return loader;
	}

	/**
	 * Stands in for any class loader a project class is handed, such as the result
	 * of {@code getClassLoader()}, {@code getSystemClassLoader()} or
	 * {@code getParent()}. A sandbox loader is passed through. Any other loader is
	 * swapped for the sandbox loader of the project class that asked.
	 *
	 * @param candidate the loader the call returned
	 * @param caller    the project class that asked
	 * @return a sandbox loader, or null when the call returned null
	 */
	public static ClassLoader sandboxLoader(ClassLoader candidate, Class<?> caller) {
		if (candidate == null || SemossClassLoader.isSandboxLoader(candidate)) {
			return candidate;
		}
		return callerLoader(caller);
	}

	/**
	 * Checks a loader a project class cast to a specific loader type, such as
	 * {@code URLClassLoader}: only a sandbox loader passes.
	 *
	 * @param candidate the loader that was cast
	 * @return the same loader
	 * @throws SecurityException if the loader is not a sandbox loader
	 */
	public static ClassLoader requireSandboxLoader(ClassLoader candidate) {
		if (candidate != null && !SemossClassLoader.isSandboxLoader(candidate)) {
			throw new SecurityException("This class loader is not available in project reactors");
		}
		return candidate;
	}

	/**
	 * Sandboxed replacement for {@code new URLClassLoader(URL[])} and
	 * {@code URLClassLoader.newInstance(URL[])}: a loader for local jars under the
	 * sandbox loader of the project class that asked.
	 *
	 * @param urls   the local jars and folders
	 * @param caller the project class that asked
	 * @return the sandbox loader
	 */
	public static URLClassLoader urlClassLoader(URL[] urls, Class<?> caller) {
		return new SemossProjectJarClassLoader(urls, caller);
	}

	/**
	 * Sandboxed replacement for {@code new URLClassLoader(URL[], ClassLoader)} and
	 * {@code URLClassLoader.newInstance(URL[], ClassLoader)}.
	 *
	 * @param urls            the local jars and folders
	 * @param requestedParent the parent the project class asked for, which is not
	 *                        used
	 * @param caller          the project class that asked
	 * @return the sandbox loader
	 */
	public static URLClassLoader urlClassLoader(URL[] urls, ClassLoader requestedParent, Class<?> caller) {
		return new SemossProjectJarClassLoader(urls, caller);
	}

	/**
	 * Sandboxed replacement for
	 * {@code new URLClassLoader(URL[], ClassLoader, URLStreamHandlerFactory)}.
	 *
	 * @param urls             the local jars and folders
	 * @param requestedParent  the parent the project class asked for, which is not
	 *                         used
	 * @param requestedFactory the URL handler factory the project class asked for,
	 *                         which is not used
	 * @param caller           the project class that asked
	 * @return the sandbox loader
	 */
	public static URLClassLoader urlClassLoader(URL[] urls, ClassLoader requestedParent,
			URLStreamHandlerFactory requestedFactory, Class<?> caller) {
		return new SemossProjectJarClassLoader(urls, caller);
	}

	/**
	 * Sandboxed replacement for
	 * {@code new URLClassLoader(String, URL[], ClassLoader)}.
	 *
	 * @param name            the loader's name
	 * @param urls            the local jars and folders
	 * @param requestedParent the parent the project class asked for, which is not
	 *                        used
	 * @param caller          the project class that asked
	 * @return the sandbox loader
	 */
	public static URLClassLoader urlClassLoader(String name, URL[] urls, ClassLoader requestedParent, Class<?> caller) {
		return new SemossProjectJarClassLoader(name, urls, requestedParent, caller);
	}

	/**
	 * Sandboxed replacement for
	 * {@code new URLClassLoader(String, URL[], ClassLoader, URLStreamHandlerFactory)}.
	 *
	 * @param name             the loader's name
	 * @param urls             the local jars and folders
	 * @param requestedParent  the parent the project class asked for, which is not
	 *                         used
	 * @param requestedFactory the URL handler factory the project class asked for,
	 *                         which is not used
	 * @param caller           the project class that asked
	 * @return the sandbox loader
	 */
	public static URLClassLoader urlClassLoader(String name, URL[] urls, ClassLoader requestedParent,
			URLStreamHandlerFactory requestedFactory, Class<?> caller) {
		return new SemossProjectJarClassLoader(name, urls, requestedParent, caller);
	}

	/**
	 * The jars and folders a project class asked a loader to read, each checked by
	 * {@link #sandboxedJarUrl(URL)}.
	 *
	 * @param urls the jars and folders
	 * @return the same locations, under the path rule
	 */
	static URL[] sandboxedJarUrls(URL[] urls) {
		URL[] sandboxed = new URL[urls.length];
		for (int i = 0; i < urls.length; i++) {
			sandboxed[i] = sandboxedJarUrl(urls[i]);
		}
		return sandboxed;
	}

	/**
	 * A jar or folder a project class asked a loader to read. It has to be on the
	 * local disk, and it follows the same path rule as {@link SandboxedJavaExecution#file(String)}.
	 *
	 * @param url the jar or folder
	 * @return the same location, under the path rule
	 * @throws SecurityException if the location is not a local file
	 */
	static URL sandboxedJarUrl(URL url) {
		if (!"file".equalsIgnoreCase(url.getProtocol())) {
			throw new SecurityException("Only jars and folders on the local disk can be loaded in project reactors");
		}
		try {
			String location = new File(SandboxedJavaExecution.sanitize(Path.of(url.toURI()).toString())).toURI().toString();
			if (url.getPath().endsWith("/") && !location.endsWith("/")) {
				location += "/";
			}
			return URI.create(location).toURL();
		} catch (URISyntaxException | MalformedURLException | IllegalArgumentException e) {
			throw new SecurityException("Not a local jar or folder: " + e.getMessage(), e);
		}
	}
}
