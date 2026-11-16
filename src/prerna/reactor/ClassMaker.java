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
package prerna.reactor;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javassist.CannotCompileException;
import javassist.ClassClassPath;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtNewMethod;
import javassist.NotFoundException;
import prerna.util.Console;
import prerna.util.Utility;

/**
 * Builds a Java class at runtime with javassist. Callers set a super class, add
 * fields and methods as Java source, and load the result with
 * {@link #toClass()}. Pixel lambdas, assimilators and the runtime class builder
 * use it to turn generated code into a class they can instantiate.
 * <p>
 * Every class is made in the JVM wide default {@link ClassPool}. The pool's
 * class path and the packages generated code can use without qualifying them
 * are set up once, and each package from {@link #addImport(String)} is imported
 * once.
 */
public class ClassMaker {

	private static final Logger classLogger = LogManager.getLogger(ClassMaker.class);

	private static final ClassPool POOL = ClassPool.getDefault();
	private static final Set<String> IMPORTED_PACKAGES = new HashSet<>();
	private static final String[] DEFAULT_PACKAGES = { "java.sql", "java.lang", "java.util", "prerna.reactor",
			"prerna.engine.api", "prerna.util", "org.apache.tinkerpop.gremlin.process.traversal",
			"org.apache.tinkerpop.gremlin.structure", "prerna.ds", "prerna.om" };

	static {
		POOL.insertClassPath(new ClassClassPath(ClassMaker.class));
		POOL.insertClassPath(new ClassClassPath(Console.class));
		for (String packageName : DEFAULT_PACKAGES) {
			importPackage(packageName);
		}
	}

	private final CtClass cc;
	private boolean hasSuper = false;

	/**
	 * Starts a class with a random package and a random class name.
	 */
	public ClassMaker() {
		this("t" + Utility.getRandomString(12), "c" + Utility.getRandomString(12));
	}

	/**
	 * Starts a class in the given package, named c followed by the class name.
	 *
	 * @param packageName the package of the generated class
	 * @param className   the class name, which gets a c prefix
	 */
	public ClassMaker(String packageName, String className) {
		this.cc = POOL.makeClass(packageName + ".c" + className);
	}

	/**
	 * Adds an interface the generated class implements.
	 *
	 * @param interfaceName the fully qualified name of the interface
	 * @throws IllegalArgumentException when the interface cannot be found
	 */
	public void addInterface(String interfaceName) {
		try {
			cc.addInterface(POOL.getCtClass(interfaceName));
		} catch (NotFoundException e) {
			throw new IllegalArgumentException(
					"Could not find interface " + interfaceName + " for the generated class: " + e.getMessage(), e);
		}
	}

	/**
	 * Sets the super class of the generated class. Only the first call sets it, and
	 * later calls are ignored.
	 *
	 * @param superClassName the fully qualified name of the super class
	 * @throws IllegalArgumentException when the super class cannot be found or set
	 */
	public void addSuper(String superClassName) {
		if (hasSuper) {
			return;
		}
		try {
			cc.setSuperclass(POOL.getCtClass(superClassName));
		} catch (NotFoundException | CannotCompileException e) {
			throw new IllegalArgumentException(
					"Could not set super class " + superClassName + " on the generated class: " + e.getMessage(), e);
		}
		hasSuper = true;
	}

	/**
	 * Compiles a method from its Java source and adds it to the generated class.
	 *
	 * @param methodSource the full method source, from its modifiers to its closing
	 *                     brace
	 * @throws IllegalArgumentException when the method does not compile, with the
	 *                                  compiler's reason in the message
	 */
	public void addMethod(String methodSource) {
		try {
			cc.addMethod(CtNewMethod.make(methodSource, cc));
		} catch (CannotCompileException e) {
			throw new IllegalArgumentException("Could not compile the generated method: " + e.getMessage(), e);
		}
	}

	/**
	 * Compiles a field from its Java source and adds it to the generated class.
	 *
	 * @param fieldSource the full field declaration, such as
	 *                    {@code private int count = 0;}
	 * @throws IllegalArgumentException when the field does not compile, with the
	 *                                  compiler's reason in the message
	 */
	public void addField(String fieldSource) {
		try {
			cc.addField(CtField.make(fieldSource, cc));
		} catch (CannotCompileException e) {
			throw new IllegalArgumentException("Could not compile the generated field: " + e.getMessage(), e);
		}
	}

	/**
	 * Lets generated code use the classes of a package without qualifying them. The
	 * package is imported into the shared class pool, so every class made afterward
	 * can use it too.
	 *
	 * @param packageName the package to import
	 */
	public void addImport(String packageName) {
		importPackage(packageName);
	}

	/**
	 * Loads the generated class and removes it from the class pool, so the pool
	 * does not hold every class made at runtime. The maker cannot change or write
	 * the class afterward, and later generated classes cannot extend it. Use
	 * {@link #toExtendableClass()} for a class that later classes extend.
	 *
	 * @return the loaded class
	 * @throws IllegalStateException when the class cannot be compiled or loaded
	 */
	public Class<?> toClass() {
		try {
			return loadClass();
		} finally {
			cc.detach();
		}
	}

	/**
	 * Loads the generated class and keeps it in the class pool, so later generated
	 * classes can extend it through {@link #addSuper(String)}.
	 *
	 * @return the loaded class
	 * @throws IllegalStateException when the class cannot be compiled or loaded
	 */
	public Class<?> toExtendableClass() {
		return loadClass();
	}

	/**
	 * Writes the generated class file, which helps debug generated code. Call it
	 * before the class is loaded.
	 *
	 * @param fileName the path of the class file to write
	 */
	public void writeClass(String fileName) {
		try (DataOutputStream out = new DataOutputStream(new FileOutputStream(fileName))) {
			cc.getClassFile().write(out);
		} catch (IOException e) {
			classLogger.error("Could not write generated class {} to {}", cc.getName(), fileName, e);
		}
	}

	/**
	 * Compiles and loads the generated class.
	 *
	 * @return the loaded class
	 * @throws IllegalStateException when the class cannot be compiled or loaded
	 */
	private Class<?> loadClass() {
		try {
			return cc.toClass();
		} catch (CannotCompileException e) {
			throw new IllegalStateException("Could not load generated class " + cc.getName(), e);
		}
	}

	/**
	 * Imports a package into the shared class pool, once.
	 *
	 * @param packageName the package to import
	 */
	private static void importPackage(String packageName) {
		synchronized (IMPORTED_PACKAGES) {
			if (IMPORTED_PACKAGES.add(packageName)) {
				POOL.importPackage(packageName);
			}
		}
	}
}
