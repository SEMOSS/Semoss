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

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xeustechnologies.jcl.JarClassLoader;

/**
 * A restricted subclass of {@link JarClassLoader} used for loading Maven-based
 * project reactor JARs. Applies the same security restrictions as
 * {@link SemossClassLoader} for folder-based reactor classes:
 *
 * <ul>
 * <li>Blocks loading of dangerous classes (reflection, process execution,
 * networking, script engines, etc.) via
 * {@link SemossClassLoader#isReflectionApiBlocked(String)}.</li>
 * <li>Forces all {@code prerna.*} classes to load from the application
 * classloader, preventing spoofed prerna classes in reactor JARs.</li>
 * <li>Applies bytecode transformation to every class loaded from the JAR,
 * intercepting {@code System.getenv}, {@code System.exit}, file paths, network
 * access, and all other sandbox-enforced APIs - identical to the transformation
 * applied by {@link SemossClassLoader}.</li>
 * </ul>
 *
 * @see SemossClassLoader#isReflectionApiBlocked(String)
 * @see SemossClassLoader#applyBytecodeTransformations(byte[])
 */
public class SemossJarClassLoader extends JarClassLoader {

	private static final Logger classLogger = LogManager.getLogger(SemossJarClassLoader.class);

	/**
	 * Constructs a SemossJarClassLoader and registers additional system properties
	 * that reactor code loaded from the JAR is permitted to read.
	 *
	 * @param additionalProperties extra properties to expose to reactor code;
	 *                             {@code null} is treated as an empty map
	 * @see SemossClassLoader#SemossClassLoader(ClassLoader, Map)
	 */
	public SemossJarClassLoader(Map<String, String> additionalProperties) {
		SandboxedJavaExecution.addPlatformProperties(additionalProperties);
	}

	@Override
	@SuppressWarnings("rawtypes")
	public Class loadClass(String className, boolean resolveIt) throws ClassNotFoundException {
		if (className != null && SemossClassLoader.isReflectionApiBlocked(className)) {
			throw new ClassNotFoundException("Access to '" + className + "' is not permitted in project reactors");
		}
		if (className != null && className.startsWith("prerna.")) {
			// Force all prerna.* classes to load from the application classloader only.
			// This prevents a project reactor JAR from shipping a spoofed prerna.* class
			// (e.g. prerna.util.SomeMaliciousClass) to bypass SystemEngineRegistry access
			// controls. Legitimate prerna.* classes from the application classpath are
			// still resolved correctly via the application classloader.
			return SemossJarClassLoader.class.getClassLoader().loadClass(className);
		}
		return super.loadClass(className, resolveIt);
	}

	/**
	 * Intercepts raw class bytecode read from the JAR before it is passed to
	 * {@code defineClass}, and applies the same sandbox bytecode transformations
	 * used by {@link SemossClassLoader}. This ensures that classes packaged in a
	 * reactor JAR are subject to exactly the same runtime restrictions as classes
	 * loaded from a reactor folder.
	 */
	@Override
	protected byte[] loadClassBytes(String className) {
		byte[] bytes = super.loadClassBytes(className);
		if (bytes == null) {
			return null;
		}
		try {
			return SemossClassLoader.applyBytecodeTransformations(bytes);
		} catch (Exception e) {
			classLogger.error("Failed to apply sandbox transformations to JAR class '{}' - denying load", className, e);
			return null;
		}
	}

}
