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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A custom class loader for dynamically loading project-specific classes in
 * Semoss. This class loader allows the application to load compiled Java
 * classes (.class files) from a specified folder that is not on the default
 * application classpath. This is essential for loading custom reactors and
 * other project-specific code at runtime.
 */
public class SemossClassloader extends ClassLoader {

	private static final Logger classLogger = LogManager.getLogger(SemossClassloader.class);

	private String folder = null;

	/**
	 * Constructs a new SemossClassloader with a specified parent class loader.
	 * 
	 * @param parent The parent class loader.
	 */
	public SemossClassloader(ClassLoader parent) {
		super(parent);
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
				Class<?> c = defineClass(name, b, 0, b.length);
				resolveClass(c);
				return c;
			}
		} catch (IOException e) {
			classLogger.error("Failed to read class file for '{}' from folder '{}'", name, folder, e);
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
		return name.startsWith("java.lang.reflect.") || name.equals("java.lang.invoke.MethodHandles")
				|| name.equals("java.lang.invoke.MethodHandles$Lookup") || name.equals("sun.misc.Unsafe")
				|| name.equals("jdk.internal.misc.Unsafe");
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

}
