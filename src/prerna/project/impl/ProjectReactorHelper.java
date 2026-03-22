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
package prerna.project.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.maven.shared.invoker.DefaultInvocationRequest;
import org.apache.maven.shared.invoker.DefaultInvoker;
import org.apache.maven.shared.invoker.InvocationRequest;
import org.apache.maven.shared.invoker.InvocationResult;
import org.apache.maven.shared.invoker.Invoker;
import org.apache.maven.shared.invoker.MavenInvocationException;
import org.xeustechnologies.jcl.JclObjectFactory;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import javassist.ClassPool;
import prerna.engine.impl.SmssUtilities;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.reactor.IReactor;
import prerna.reactor.frame.AbstractFrameReactor;
import prerna.reactor.frame.py.AbstractPyFrameReactor;
import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.reactor.task.TaskBuilderReactor;
import prerna.util.CmdExecUtil;
import prerna.util.Constants;
import prerna.util.SemossClassLoader;
import prerna.util.SemossJarClassLoader;
import prerna.util.Settings;
import prerna.util.Utility;
import prerna.util.git.GitAssetUtils;

/**
 * Helper class for managing and loading project-specific reactors. This class
 * provides functionality to load reactors from various sources such as folders,
 * JAR files, and Maven projects. It uses different class loaders to isolate
 * project-specific dependencies and avoid conflicts.
 */
public class ProjectReactorHelper {

	/**
	 * Custom reactors must never declare a package inside the core prerna.*
	 * namespace. A class that does so could spoof the StackWalker package check in
	 * SystemEngineRegistry and gain access to system engines it is not authorised
	 * to touch. Any class whose fully-qualified name starts with this prefix is
	 * rejected at load time.
	 */
	private static final String PROTECTED_PACKAGE_PREFIX = "prerna.";

	private static final Logger classLogger = LogManager.getLogger(ProjectReactorHelper.class);
	private static final String DIR_SEPARATOR = "/";

	private SemossClassLoader projectClassLoader = null;

	// for jars
	private URLClassLoader urlClassLoader;
	// for pom
	private boolean mvnDefined = false;
	private SemossJarClassLoader mvnClassLoader = null;

	private IProject project = null;

	/**
	 * Constructs a new ProjectReactorHelper for a given project.
	 *
	 * @param project The project for which to manage reactors.
	 */
	public ProjectReactorHelper(IProject project) {
		this.project = project;
	}

	/**
	 * Loads reactors from a specified folder within the project. Assumes the
	 * compiled classes are in a "classes" subfolder.
	 *
	 * @param folder The base folder to search for reactors.
	 * @return A map of reactor names to their corresponding classes.
	 */
	public Map<String, Class<IReactor>> loadReactors(String folder) {
		return loadReactors(folder, "classes");
	}

	/**
	 * Loads reactors from a specified folder and output subfolder within the
	 * project.
	 *
	 * @param folder       The base folder to search for reactors.
	 * @param outputFolder The subfolder containing the compiled class files.
	 * @return A map of reactor names to their corresponding classes.
	 */
	// loads classes through this specific class loader for the insight
	public Map<String, Class<IReactor>> loadReactors(String folder, String outputFolder) {
		projectClassLoader = new SemossClassLoader(ProjectReactorHelper.class.getClassLoader());

		Map<String, Class<IReactor>> reactorMap = new HashMap<>();
		String disable_terminal = Utility.getDIHelperProperty(Constants.DISABLE_TERMINAL);
		if (disable_terminal != null && !disable_terminal.isEmpty()) {
			if (Boolean.parseBoolean(disable_terminal)) {
				classLogger.debug("Project specific reactors are disabled");
				return reactorMap;
			}
		}
		try {
			// the main folder to add here is
			// basefolder/db/insightfolder/classes
			String classesFolder = folder + "/" + outputFolder;

			classesFolder = classesFolder.replace("\\", "/");
			projectClassLoader.setFolder(classesFolder);

			File file = new File(classesFolder);
			if (file.exists()) {
				classLogger.info("Loading reactors from >> " + classesFolder);

				Map<String, List<String>> dirs = GitAssetUtils.browse(classesFolder, classesFolder);
				List<String> dirList = dirs.get("DIR_LIST");

				String[] packages = new String[dirList.size()];
				for (int dirIndex = 0; dirIndex < dirList.size(); dirIndex++) {
					packages[dirIndex] = dirList.get(dirIndex);
				}

				ScanResult sr = new ClassGraph().overrideClasspath((new File(classesFolder).toURI().toURL()))
						.enableClassInfo().whitelistPackages(packages).scan();

				// find everything implementing IReactor
				// get implementing classes doesn't seem to work when overriding the classpath
				// likely because the base semoss classes are not in the scope of the ClassGraph
				// object
				ClassInfoList classes = sr.getAllClasses();
				for (int classIndex = 0; classIndex < classes.size(); classIndex++) {
					ClassInfo classObject = classes.get(classIndex);
					String className = classObject.getName();
					if (isProtectedPackage(className)) {
						classLogger.warn("Blocked custom reactor with protected package name: {}", className);
						continue;
					}

					if (!classObject.isInterface() && !classObject.isAbstract() && classObject.isPublic()
							&& isValidReactor(classObject)) {
						Class<IReactor> actualClass = (Class<IReactor>) projectClassLoader.loadClass(className);

						String reactorName = classes.get(classIndex).getSimpleName();
						final String REACTOR_KEY = "REACTOR";
						if (reactorName.toUpperCase().endsWith(REACTOR_KEY)) {
							reactorName = reactorName.substring(0, reactorName.length() - REACTOR_KEY.length());
						}

						reactorMap.put(reactorName.toUpperCase(), actualClass);
					}
				}
			}
		} catch (Exception ex) {
			classLogger.error("Failed to load reactors from folder '{}' for project '{}'", folder,
					project.getProjectId(), ex);
		}

		return reactorMap;
	}

	/**
	 * Loads reactors from a Maven project's pom.xml file. It resolves dependencies
	 * and loads the reactor classes.
	 *
	 * @param folder       The base folder of the Maven project.
	 * @param outputFolder The target folder where compiled classes are located.
	 * @return A map of reactor names to their corresponding classes.
	 */
	// loads classes through this specific class loader for the insight
	public Map<String, Class<IReactor>> loadReactorsFromPom(String folder, String outputFolder) {
		Map<String, Class<IReactor>> reactors = new HashMap<>();
		String disable_terminal = Utility.getDIHelperProperty(Constants.DISABLE_TERMINAL);
		if (disable_terminal != null && !disable_terminal.isEmpty()) {
			if (Boolean.parseBoolean(disable_terminal)) {
				classLogger.debug("Project specific reactors are disabled");
				return reactors;
			}
		}

		try {
			// I should create the class pool everytime
			// this way it doesn't keep others and try to get from other places
			// does this end up loading all the other classes too ?
			ClassPool pool = ClassPool.getDefault();
			// takes a class and modifies the name of the package and then plugs it into the
			// heap

			// the main folder to add here is
			// basefolder/db/insightfolder/classes - right now I have it as classes. we can
			// change it to something else if we want
			String classesFolder = folder + "/" + outputFolder;

			classesFolder = classesFolder.replaceAll("\\\\", "/");
			this.mvnClassLoader.add(classesFolder);

			File file = new File(classesFolder);
			if (file.exists()) {
				// loads a class and tried to change the package of the class on the fly
				// CtClass clazz = pool.get("prerna.test.CPTest");

				classLogger.info("Loading reactors from >> " + classesFolder);

				Map<String, List<String>> dirs = GitAssetUtils.browse(classesFolder, classesFolder);
				List<String> dirList = dirs.get("DIR_LIST");

				// get the directories before scanning
				String[] packages = new String[dirList.size()];
				for (int dirIndex = 0; dirIndex < dirList.size(); dirIndex++) {
					packages[dirIndex] = dirList.get(dirIndex);
				}

				ScanResult sr = new ClassGraph().overrideClasspath((new File(classesFolder).toURI().toURL()))
						.whitelistPackages(packages).scan();

				String[] subclassSearch = new String[] { AbstractReactor.class.getName(),
						prerna.sablecc2.reactor.AbstractReactor.class.getName(), };

				for (String sublcass : subclassSearch) {
					ClassInfoList classes = sr.getSubclasses(sublcass);
					// add the path to the insight classes so only this guy can load it
					pool.insertClassPath(classesFolder);

					for (int classIndex = 0; classIndex < classes.size(); classIndex++) {
						String mvnClassName = classes.get(classIndex).getName();
						if (isProtectedPackage(mvnClassName)) {
							classLogger.warn("Blocked custom reactor with protected package name: {}", mvnClassName);
							continue;
						}
						// this will load the reactor with everything
						JclObjectFactory factory = JclObjectFactory.getInstance();

						// Create object of loaded class
						Object loadedObject = factory.create(this.mvnClassLoader, mvnClassName);

						String reactorName = classes.get(classIndex).getSimpleName();
						final String REACTOR_KEY = "REACTOR";
						if (reactorName.toUpperCase().endsWith(REACTOR_KEY)) {
							reactorName = reactorName.substring(0, reactorName.length() - REACTOR_KEY.length());
						}

						reactors.put(reactorName.toUpperCase(), (Class<IReactor>) loadedObject.getClass());
					}
				}
			}
		} catch (Exception ex) {
			classLogger.error("Failed to load reactors from pom '{}' for project '{}'", folder, project.getProjectId(),
					ex);
		}

		return reactors;
	}

	/**
	 * Load reactors directly from a compiled jar(s)
	 * 
	 * @param urls An array of URLs pointing to the JAR files.
	 * @return A map of reactor names to their corresponding classes.
	 */
	public Map<String, Class<IReactor>> loadReactorsFromJars(URL[] urls) {
		Map<String, Class<IReactor>> reactorsMap = new HashMap<>();
		String disable_terminal = Utility.getDIHelperProperty(Constants.DISABLE_TERMINAL);
		if (disable_terminal != null && !disable_terminal.isEmpty()) {
			if (Boolean.parseBoolean(disable_terminal)) {
				classLogger.debug("Project specific reactors are disabled");
				return reactorsMap;
			}
		}

		if (urlClassLoader != null) {
			try {
				urlClassLoader.close();
			} catch (IOException e) {
				// can ignore
				classLogger.error("Error closing existing jar class loader: " + e.getMessage(), e);
			}
		}

		projectClassLoader = new SemossClassLoader(ProjectReactorHelper.class.getClassLoader());
		urlClassLoader = new URLClassLoader(urls, this.projectClassLoader);
		try {
			// scan all abstract reactors
			ScanResult sr = new ClassGraph().overrideClasspath((Object[]) urls).enableClassInfo().scan();

			// find everything implementing IReactor
			// get implementing classes doesn't seem to work when overriding the classpath
			// likely because the base semoss classes are not in the scope of the ClassGraph
			// object
			ClassInfoList classes = sr.getAllClasses();
			for (int classIndex = 0; classIndex < classes.size(); classIndex++) {
				ClassInfo classObject = classes.get(classIndex);
				String className = classObject.getName();

				if (!classObject.isInterface() && !classObject.isAbstract() && classObject.isPublic()
						&& isValidReactor(classObject)) {
					if (isProtectedPackage(className)) {
						classLogger.warn("Blocked custom reactor with protected package name: {}", className);
						continue;
					}
					Class<IReactor> actualClass = (Class<IReactor>) urlClassLoader.loadClass(className);

					String reactorName = classes.get(classIndex).getSimpleName();
					final String REACTOR_KEY = "REACTOR";
					if (reactorName.toUpperCase().endsWith(REACTOR_KEY)) {
						reactorName = reactorName.substring(0, reactorName.length() - REACTOR_KEY.length());
					}

					reactorsMap.put(reactorName.toUpperCase(), actualClass);
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to load reactors from JARs for project '{}'", project.getProjectId(), e);
		}

		return reactorsMap;
	}

	/**
	 * Creates a Maven class loader for the project. This method initializes a
	 * JarClassLoader and populates it with dependencies from the project's pom.xml.
	 *
	 * @param pomFile The pom.xml file of the Maven project.
	 */
	public void makeMvnClassloader(File pomFile) {
		if (mvnClassLoader == null) {
			// get all the new jars first
			// to add to the classloader
			String mvnHome = System.getProperty(Settings.MVN_HOME);
			if (mvnHome == null) {
				mvnHome = Utility.getDIHelperProperty(Settings.MVN_HOME);
			}
			if (mvnHome == null) {
				mvnDefined = true;
				return;
			}

			// now load the classloader
			// add the jars
			// locate all the reactors
			// and keep access to it
			mvnClassLoader = new SemossJarClassLoader(Map.of());

			// classes are in
			// appRoot / classes
			// get the libraries
			// run maven dependency:list to get all the dependencies and process
			List<String> classpaths = composeClasspath(pomFile, mvnHome);
			if (classpaths != null) {
				for (int classPathIndex = 0; classPathIndex < classpaths.size(); classPathIndex++) {
					// add all the libraries
					mvnClassLoader.add(classpaths.get(classPathIndex));
				}
			}
		}
	}

	/**
	 * Composes the classpath for a Maven project by resolving its dependencies.
	 *
	 * @param pomFile The pom.xml file of the Maven project.
	 * @param mvnHome The path to the Maven home directory.
	 * @return A list of classpath entries (JAR file paths).
	 */
	private List<String> composeClasspath(File pomFile, String mvnHome) {
		BufferedReader br = null;
		try {
			File outputFile = new File(pomFile.getParent() + DIR_SEPARATOR + "mvn_dep.output");
			boolean built = false;
			if (mvnHome != null) {

				// run this only if mvn dependencies have been wiped out
				if (outputFile.exists()) {
					built = true;
				} else {
					InvocationRequest request = new DefaultInvocationRequest();
					// request.
					request.setPomFile(pomFile);
					request.setMavenOpts("-DoutputType=graphml -DoutputFile=\"" + outputFile.getAbsolutePath()
							+ "\" -DincludeScope=runtime ");
					request.setGoals(Collections.singletonList("dependency:list"));

					Invoker invoker = new DefaultInvoker();
					invoker.setWorkingDirectory(pomFile.getParentFile());
					invoker.setMavenHome(new File(Utility.normalizePath(mvnHome)));
					InvocationResult result = invoker.execute(request);

					if (result.getExitCode() != 0) {
						built = false;
						// throw new IllegalStateException( "Build failed." );
					}
				}
			}

			if (!built) { // may be maven is not set but mvn as a executor is available
				// need to make the modification to this
				CmdExecUtil ceu = new CmdExecUtil(null,
						SmssUtilities.getUniqueName(project.getProjectName(), project.getProjectId()),
						pomFile.getParent());
				// mvn dependency:list -DoutputType=graphml -DoutputFile=./mvn_dep.output
				// -DincludeScope=runtime -f pom.xml
				ceu.executeCommand("mvn dependency:list -DoutputType=graphml -DoutputFile=\""
						+ outputFile.getAbsolutePath() + "\" -DincludeScope=runtime -f \"" + pomFile + "\"");
			}
			// now process the dependency list
			// and then delete it
			// otherwise we have the list
			String repoHome = System.getProperty(Settings.REPO_HOME);
			if (repoHome == null) {
				repoHome = Utility.getDIHelperProperty(Settings.REPO_HOME);
			}
			if (repoHome == null) {
				mvnDefined = true;
				return null;
			}

			List<String> finalCP = new ArrayList<>();
			br = new BufferedReader(new InputStreamReader(new FileInputStream(outputFile)));
			String data = null;
			while ((data = br.readLine()) != null) {
				if (data.endsWith("compile")) {
					String[] pathTokens = data.split(":");

					String baseDir = pathTokens[0];
					String packageName = pathTokens[1];
					String version = pathTokens[3];

					baseDir = repoHome + "/" + baseDir.replace(".", "/").trim();
					finalCP.add(baseDir + DIR_SEPARATOR + packageName + DIR_SEPARATOR + version + DIR_SEPARATOR
							+ packageName + "-" + version + ".jar");
				}
			}

			return finalCP;
		} catch (MavenInvocationException e) {
			classLogger.error("Maven invocation failed while resolving dependencies for pom '{}'",
					pomFile.getAbsolutePath(), e);
		} catch (FileNotFoundException e) {
			classLogger.error("Maven dependency output file not found for pom '{}'", pomFile.getAbsolutePath(), e);
		} catch (IOException e) {
			classLogger.error("IO error while reading Maven dependency output for pom '{}'", pomFile.getAbsolutePath(),
					e);
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					classLogger.error("Failed to close Maven dependency output reader for pom '{}'",
							pomFile.getAbsolutePath(), e);
				}
			}
		}

		return null;
	}

	/**
	 * Checks if a Maven home directory is defined.
	 *
	 * @return true if Maven home is defined, false otherwise.
	 */
	public boolean isMvnDefined() {
		return this.mvnDefined;
	}

	/**
	 * Closes any open class loaders to release resources. This should be called
	 * when the project is unloaded or the application is shutting down.
	 */
	public void close() {
		if (urlClassLoader != null) {
			try {
				urlClassLoader.close();
			} catch (IOException e) {
				classLogger.error("Failed to close URL classloader for project '{}'", project.getProjectId(), e);
			}
		}

		urlClassLoader = null;
		mvnClassLoader = null;
		projectClassLoader = null;
	}

	/**
	 * Returns true if the fully-qualified class name falls inside the protected
	 * prerna.* namespace. Custom reactors must not declare themselves in this
	 * namespace because doing so would spoof the StackWalker package check in
	 * SystemEngineRegistry and grant them unauthorised access to system engines.
	 */
	private static boolean isProtectedPackage(String className) {
		return className.startsWith(PROTECTED_PACKAGE_PREFIX);
	}

	/**
	 * Checks if a given class is a valid reactor. A valid reactor is a class that
	 * implements the IReactor interface or extends a known reactor base class.
	 *
	 * @param classObject The ClassInfo object representing the class to check.
	 * @return true if the class is a valid reactor, false otherwise.
	 */
	private static boolean isValidReactor(ClassInfo classObject) {
		String className = classObject.getName();
		if (className.equals(AbstractRFrameReactor.class.getName())
				|| className.equals(AbstractPyFrameReactor.class.getName())
				|| className.equals(AbstractFrameReactor.class.getName())
				|| className.equals(TaskBuilderReactor.class.getName())
				|| className.equals(AbstractReactor.class.getName()) || className.equals(IReactor.class.getName())
				|| className.equals(prerna.sablecc2.reactor.AbstractReactor.class.getName())) {
			return true;
		}
		if (classObject.implementsInterface(IReactor.class.getName())) {
			return true;
		}

		ClassInfo superClass = classObject.getSuperclass();
		if (superClass == null) {
			return false;
		}

		return isValidReactor(superClass);
	}
}
