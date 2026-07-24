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
import java.lang.reflect.Proxy;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;

/**
 * Secure registry for internal system engines
 *
 * Access to each engine is restricted to a per-engine allowlist of trusted
 * packages, enforced at runtime via StackWalker. Custom or third-party reactors
 * loaded through ProjectReactorHelper are denied access regardless of what
 * package name they declare.
 *
 * Registration is one-time: once an engine is registered it cannot be
 * overwritten. Registration is itself access-controlled to
 * startup/initialization packages.
 *
 */
public final class SystemEngineRegistry {

	private static final Logger classLogger = LogManager.getLogger(SystemEngineRegistry.class);

	private static final StackWalker WALKER = StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);

	/**
	 * The JVM's two built-in trusted classloaders. Any class whose loader is
	 * exactly one of these (or null = bootstrap) was loaded from the application
	 * classpath and is considered trusted. Anything else like SemossClassLoader,
	 * URLClassLoader, JarClassLoader, or any other user-defined loader is untrusted
	 * regardless of the package name it declares.
	 */
	private static final ClassLoader PLATFORM_CL = ClassLoader.getPlatformClassLoader();
	private static final ClassLoader SYSTEM_CL = ClassLoader.getSystemClassLoader();

	/**
	 * The classloader that loaded this class. In Tomcat this is
	 * ParallelWebappClassLoader; in tests it is the system classloader. All classes
	 * loaded from the same WAR (i.e. WEB-INF/lib and WEB-INF/classes) share this
	 * loader instance
	 */
	private static final ClassLoader WEBAPP_CL = SystemEngineRegistry.class.getClassLoader();

	/*
	 * We have a per-engine caller allowlist (prefix based: covers all subpackages)
	 */

	private static final Set<String> SECURITY_DB_ALLOWED = Set.of("prerna.auth", "prerna.reactor.security",
			"prerna.reactor.appprofile", "prerna.reactor.platformprofile",
			"prerna.semoss.web.services.config", "prerna.util", "prerna.web.conf");

	private static final Set<String> LOCAL_MASTER_DB_ALLOWED = Set.of("prerna.auth", "prerna.masterdatabase",
			"prerna.reactor.masterdatabase", "prerna.reactor.utils", "prerna.util", "prerna.web.conf");

	private static final Set<String> SCHEDULER_DB_ALLOWED = Set.of("prerna.auth", "prerna.reactor.scheduler",
			"prerna.util", "prerna.web.conf");

	private static final Set<String> THEMING_DB_ALLOWED = Set.of("prerna.auth", "prerna.theme", "prerna.util",
			"prerna.web.conf");

	private static final Set<String> USER_TRACKING_DB_ALLOWED = Set.of("prerna.auth", "prerna.usertracking",
			"prerna.util", "prerna.web.conf");

	private static final Set<String> PROMPT_DB_ALLOWED = Set.of("prerna.auth", "prerna.prompt", "prerna.util",
			"prerna.web.conf");

	private static final Set<String> NOTIFICATION_DB_ALLOWED = Set.of("prerna.auth", "prerna.notifications",
			"prerna.util", "prerna.web.conf");

	private static final Set<String> AUDIT_LOGS_DB_ALLOWED = Set.of("prerna.auth", "prerna.engine.logging",
			"prerna.logging", "prerna.util", "prerna.web.conf");

	private static final Set<String> MODEL_INFERENCE_LOGS_DB_ALLOWED = Set.of("prerna.auth",
			"prerna.engine.impl.model.inferencetracking", "prerna.reactor.agent.run", "prerna.util", "prerna.web.conf");

	/**
	 * Registration allowlist - the startup/init classes that can register an engine
	 */
	private static final Set<String> REGISTRATION_ALLOWED = Set.of("prerna.util");

	/**
	 * The canonical set of system engine IDs, keyed by their Constants values. Used
	 * to guard Utility.loadEngine() from accidentally loading these engines through
	 * the normal path.
	 */
	static final Set<String> SYSTEM_ENGINE_IDS = Set.of(Constants.SECURITY_DB, Constants.LOCAL_MASTER_DB,
			Constants.SCHEDULER_DB, Constants.THEMING_DB, Constants.USER_TRACKING_DB, Constants.PROMPT_DB,
			Constants.NOTIFICATION_DB, Constants.AUDIT_LOGS_DB, Constants.MODEL_INFERENCE_LOGS_DB);

	/**
	 * Cache of proxy-verified callers. The classloader check result is stable for
	 * any given caller Class, so a positive result is cached permanently. Only
	 * positive results are cached; denied accesses always throw immediately.
	 */
	private static final java.util.concurrent.ConcurrentHashMap<Class<?>, Boolean> ACCESS_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

	private static volatile Supplier<IRDBMSEngine> securityDbHolder;
	private static volatile Supplier<IRDBMSEngine> localMasterDbHolder;
	private static volatile Supplier<IRDBMSEngine> schedulerDbHolder;
	private static volatile Supplier<IRDBMSEngine> themesDbHolder;
	private static volatile Supplier<IRDBMSEngine> userTrackingDbHolder;
	private static volatile Supplier<IRDBMSEngine> promptDbHolder;
	private static volatile Supplier<IRDBMSEngine> notificationDbHolder;
	private static volatile Supplier<IRDBMSEngine> auditLogsDbHolder;
	private static volatile Supplier<IRDBMSEngine> modelInferenceLogsDbHolder;

	private SystemEngineRegistry() {
		throw new UnsupportedOperationException("SystemEngineRegistry is a utility class");
	}

	// -------------------------------------------------------------------------
	// Getters
	// -------------------------------------------------------------------------

	public static IRDBMSEngine getSecurityDb() {
		checkAccess(SECURITY_DB_ALLOWED, "SecurityDb");
		return securityDbHolder != null ? securityDbHolder.get() : null;
	}

	public static IRDBMSEngine getLocalMasterDb() {
		checkAccess(LOCAL_MASTER_DB_ALLOWED, "LocalMasterDb");
		return localMasterDbHolder != null ? localMasterDbHolder.get() : null;
	}

	public static IRDBMSEngine getSchedulerDb() {
		checkAccess(SCHEDULER_DB_ALLOWED, "SchedulerDb");
		return schedulerDbHolder != null ? schedulerDbHolder.get() : null;
	}

	public static IRDBMSEngine getThemesDb() {
		checkAccess(THEMING_DB_ALLOWED, "ThemesDb");
		return themesDbHolder != null ? themesDbHolder.get() : null;
	}

	public static IRDBMSEngine getUserTrackingDb() {
		checkAccess(USER_TRACKING_DB_ALLOWED, "UserTrackingDb");
		return userTrackingDbHolder != null ? userTrackingDbHolder.get() : null;
	}

	public static IRDBMSEngine getPromptDb() {
		checkAccess(PROMPT_DB_ALLOWED, "PromptDb");
		return promptDbHolder != null ? promptDbHolder.get() : null;
	}

	public static IRDBMSEngine getNotificationDb() {
		checkAccess(NOTIFICATION_DB_ALLOWED, "NotificationDb");
		return notificationDbHolder != null ? notificationDbHolder.get() : null;
	}

	public static IRDBMSEngine getAuditLogsDb() {
		checkAccess(AUDIT_LOGS_DB_ALLOWED, "AuditLogsDb");
		return auditLogsDbHolder != null ? auditLogsDbHolder.get() : null;
	}

	public static IRDBMSEngine getModelInferenceLogsDb() {
		checkAccess(MODEL_INFERENCE_LOGS_DB_ALLOWED, "ModelInferenceLogsDb");
		return modelInferenceLogsDbHolder != null ? modelInferenceLogsDbHolder.get() : null;
	}

	// -------------------------------------------------------------------------
	// Loaded-state checks: public, no access restriction, callable from anywhere
	// -------------------------------------------------------------------------

	/** Returns true if the SecurityDb has been loaded and registered. */
	public static boolean isSecurityDbLoaded() {
		return securityDbHolder != null;
	}

	/** Returns true if the LocalMasterDb has been loaded and registered. */
	public static boolean isLocalMasterDbLoaded() {
		return localMasterDbHolder != null;
	}

	/** Returns true if the SchedulerDb has been loaded and registered. */
	public static boolean isSchedulerDbLoaded() {
		return schedulerDbHolder != null;
	}

	/** Returns true if the ThemesDb has been loaded and registered. */
	public static boolean isThemesDbLoaded() {
		return themesDbHolder != null;
	}

	/** Returns true if the UserTrackingDb has been loaded and registered. */
	public static boolean isUserTrackingDbLoaded() {
		return userTrackingDbHolder != null;
	}

	/** Returns true if the PromptDb has been loaded and registered. */
	public static boolean isPromptDbLoaded() {
		return promptDbHolder != null;
	}

	/** Returns true if the NotificationDb has been loaded and registered. */
	public static boolean isNotificationDbLoaded() {
		return notificationDbHolder != null;
	}

	/** Returns true if the AuditLogsDb has been loaded and registered. */
	public static boolean isAuditLogsDbLoaded() {
		return auditLogsDbHolder != null;
	}

	/** Returns true if the ModelInferenceLogsDb has been loaded and registered. */
	public static boolean isModelInferenceLogsDbLoaded() {
		return modelInferenceLogsDbHolder != null;
	}

	/**
	 * Returns true if the given engine ID belongs to one of the known system
	 * engines. Used by Utility.loadEngine() to block the normal load path for these
	 * engines.
	 * 
	 * @param engineId
	 * @return
	 */
	public static boolean isSystemEngine(String engineId) {
		return engineId != null && SYSTEM_ENGINE_IDS.contains(engineId);
	}

	/**
	 * Loads a system engine from its SMSS file, opens it, and registers it in this
	 * registry. Does NOT add the engine to DIHelper. This is the single entry point
	 * for watcher-based startup code (e.g. SMSSWebWatcher).
	 *
	 * @param smssFilePath absolute path to the engine's .smss file
	 * @return the opened engine
	 * @throws Exception if loading or opening fails
	 */
	public static IRDBMSEngine loadSystemEngine(String smssFilePath) throws Exception {
		Properties smssProp = Utility.loadProperties(smssFilePath);
		if (smssProp == null) {
			throw new IllegalArgumentException("Unable to load SMSS file: " + smssFilePath);
		}
		String engineId = smssProp.getProperty(Constants.ENGINE);
		checkAccess(REGISTRATION_ALLOWED, engineId + " registration");
		String engineClass = smssProp.getProperty(Constants.ENGINE_TYPE);
		IEngine engine = (IEngine) Class.forName(engineClass).getDeclaredConstructor().newInstance();
		engine.setEngineId(engineId);
		engine.open(smssFilePath);
		IRDBMSEngine rdbmsEngine = (IRDBMSEngine) engine;
		registerSystemEngine(engineId, rdbmsEngine);
		return rdbmsEngine;
	}

	/**
	 * Internal registration switch, maps a system engine ID (via Constants) to the
	 * correct volatile field. Called only from loadSystemEngine.
	 * 
	 * @param engineId
	 * @param engine
	 */
	private static void registerSystemEngine(String engineId, IRDBMSEngine engine) {
		switch (engineId) {
		case Constants.SECURITY_DB -> {
			if (securityDbHolder != null) {
				throw new IllegalStateException("SecurityDb is already registered");
			}
			IRDBMSEngine guardedSecurityDb = wrapWithGuard(engine, "SecurityDb");
			securityDbHolder = () -> guardedSecurityDb;
		}
		case Constants.LOCAL_MASTER_DB -> {
			if (localMasterDbHolder != null) {
				throw new IllegalStateException("LocalMasterDb is already registered");
			}
			IRDBMSEngine guardedLocalMasterDb = wrapWithGuard(engine, "LocalMasterDb");
			localMasterDbHolder = () -> guardedLocalMasterDb;
		}
		case Constants.SCHEDULER_DB -> {
			if (schedulerDbHolder != null) {
				throw new IllegalStateException("SchedulerDb is already registered");
			}
			IRDBMSEngine guardedSchedulerDb = wrapWithGuard(engine, "SchedulerDb");
			schedulerDbHolder = () -> guardedSchedulerDb;
		}
		case Constants.THEMING_DB -> {
			if (themesDbHolder != null) {
				throw new IllegalStateException("ThemesDb is already registered");
			}
			IRDBMSEngine guardedThemesDb = wrapWithGuard(engine, "ThemesDb");
			themesDbHolder = () -> guardedThemesDb;
		}
		case Constants.USER_TRACKING_DB -> {
			if (userTrackingDbHolder != null) {
				throw new IllegalStateException("UserTrackingDb is already registered");
			}
			IRDBMSEngine guardedUserTrackingDb = wrapWithGuard(engine, "UserTrackingDb");
			userTrackingDbHolder = () -> guardedUserTrackingDb;
		}
		case Constants.PROMPT_DB -> {
			if (promptDbHolder != null) {
				throw new IllegalStateException("PromptDb is already registered");
			}
			IRDBMSEngine guardedPromptDb = wrapWithGuard(engine, "PromptDb");
			promptDbHolder = () -> guardedPromptDb;
		}
		case Constants.NOTIFICATION_DB -> {
			if (notificationDbHolder != null) {
				throw new IllegalStateException("NotificationDb is already registered");
			}
			IRDBMSEngine guardedNotificationDb = wrapWithGuard(engine, "NotificationDb");
			notificationDbHolder = () -> guardedNotificationDb;
		}
		case Constants.AUDIT_LOGS_DB -> {
			if (auditLogsDbHolder != null) {
				throw new IllegalStateException("AuditLogsDb is already registered");
			}
			IRDBMSEngine guardedAuditLogsDb = wrapWithGuard(engine, "AuditLogsDb");
			auditLogsDbHolder = () -> guardedAuditLogsDb;
		}
		case Constants.MODEL_INFERENCE_LOGS_DB -> {
			if (modelInferenceLogsDbHolder != null) {
				throw new IllegalStateException("ModelInferenceLogsDb is already registered");
			}
			IRDBMSEngine guardedModelInferenceLogsDb = wrapWithGuard(engine, "ModelInferenceLogsDb");
			modelInferenceLogsDbHolder = () -> guardedModelInferenceLogsDb;
		}
		default -> throw new IllegalArgumentException("Not a known system engine ID: " + engineId);
		}
	}

	/**
	 * Get a system engine id via its id
	 * 
	 * @param engineId
	 * @return
	 */
	public static IRDBMSEngine getSystemEngine(String engineId) {
		switch (engineId) {
		case Constants.SECURITY_DB -> {
			return getSecurityDb();
		}
		case Constants.LOCAL_MASTER_DB -> {
			return getLocalMasterDb();
		}
		case Constants.SCHEDULER_DB -> {
			return getSchedulerDb();
		}
		case Constants.THEMING_DB -> {
			return getThemesDb();
		}
		case Constants.USER_TRACKING_DB -> {
			return getUserTrackingDb();
		}
		case Constants.PROMPT_DB -> {
			return getPromptDb();
		}
		case Constants.NOTIFICATION_DB -> {
			return getNotificationDb();
		}
		case Constants.AUDIT_LOGS_DB -> {
			return getAuditLogsDb();
		}
		case Constants.MODEL_INFERENCE_LOGS_DB -> {
			return getModelInferenceLogsDb();
		}
		default -> throw new IllegalArgumentException("Not a known system engine ID: " + engineId);
		}
	}

	/**
	 * Wraps an engine in a dynamic proxy that blocks access from untrusted
	 * classloaders on every method invocation. This is a second line of defence:
	 * even if an attacker obtains the Supplier via reflection and calls get(),
	 * every subsequent method call on the returned object is still checked.
	 *
	 * The package allowlist is enforced at the getter level. The proxy only
	 * performs the classloader check, which is the one defence getters cannot
	 * provide (a reflected reference bypasses the getter entirely).
	 *
	 * InvocationTargetException is unwrapped so callers receive the original
	 * exception rather than a wrapped one.
	 *
	 * @param real the real engine instance
	 * @param name engine name used in security exception messages
	 * @return a proxy that implements IRDBMSEngine and guards every call
	 */
	private static IRDBMSEngine wrapWithGuard(IRDBMSEngine real, String name) {
		return (IRDBMSEngine) Proxy.newProxyInstance(SystemEngineRegistry.class.getClassLoader(),
				new Class<?>[] { IRDBMSEngine.class }, (proxy, method, args) -> {
					checkAccessFromProxy(name);
					try {
						return method.invoke(real, args);
					} catch (InvocationTargetException e) {
						throw e.getCause();
					}
				});
	}

	/**
	 * Variant of checkAccess used from inside the proxy InvocationHandler. The call
	 * stack contains extra JDK proxy frames between this method and the real
	 * caller, so we filter those out in addition to the usual SystemEngineRegistry
	 * frames.
	 *
	 * Stack layout (approximate): frame 0 - checkAccessFromProxy frame 1 -
	 * lambda$wrapWithGuard (InvocationHandler body, inside SystemEngineRegistry)
	 * frame 2+ - com.sun.proxy.$ProxyN / java.lang.reflect internals frame N - the
	 * actual external caller
	 *
	 * @param allowedPackages per-engine access allowlist
	 * @param context         engine name used in security exception messages
	 */
	private static void checkAccessFromProxy(String context) {
		Class<?> caller = WALKER.walk(frames -> frames.map(StackWalker.StackFrame::getDeclaringClass).filter(c -> {
			String n = c.getName();
			return !n.startsWith("prerna.util.SystemEngineRegistry") && !n.startsWith("com.sun.proxy.")
					&& !n.startsWith("jdk.proxy") && !n.startsWith("java.lang.reflect.");
		}).findFirst().orElse(null));

		if (caller == null) {
			throw new SecurityException("Unable to determine caller for " + context);
		}

		// Fast path: classloader check already passed for this caller class.
		if (ACCESS_CACHE.containsKey(caller)) {
			return;
		}

		if (isLoadedByUntrustedClassLoader(caller)) {
			classLogger.error("Unauthorized access to {} attempted from untrusted classloader: {}", context,
					caller.getName());
			throw new SecurityException("Unauthorized access to " + context
					+ " from class loaded by untrusted classloader: " + caller.getName());
		}

		// Cache the positive result so subsequent calls from this class skip the
		// classloader check entirely.
		ACCESS_CACHE.put(caller, Boolean.TRUE);
	}

	/**
	 * Walks the call stack to find the first frame outside this class, then applies
	 * two independent checks:
	 *
	 * 1. Classloader chain - if the caller's class was loaded by a
	 * SemossClassLoader (or any classloader that has one as an ancestor), it is
	 * custom/untrusted code regardless of what package name it declares. This
	 * closes the bypass where a custom JAR ships a helper class that declares
	 * "package prerna.auth;" and calls through to here - the package name would
	 * pass the allowlist, but the classloader chain reveals the true origin.
	 *
	 * 2. Package allowlist - the caller's declared package must be in the
	 * per-engine set of trusted packages.
	 *
	 * Stack layout when called from a public getter/register method: frame 0 -
	 * checkAccess (this method) frame 1 - getXyzDb / registerXyzDb frame 2 - the
	 * actual external caller (what we verify)
	 *
	 * @param allowedPackages
	 * @param context
	 */
	private static void checkAccess(Set<String> allowedPackages, String context) {
		final String registryName = SystemEngineRegistry.class.getName();

		Class<?> caller = WALKER
				.walk(frames -> frames.skip(2).map(StackWalker.StackFrame::getDeclaringClass).filter(c -> {
					String n = c.getName();
					// ignore inner classes classes properly with $
					// do not use startsWith due to matching unintended files
					return !(n.equals(registryName) || n.startsWith(registryName + "$"));
				}).findFirst().orElse(null));

		if (caller == null) {
			throw new SecurityException("Unable to determine caller for " + context);
		}

		// Check 1: classloader chain - cannot be spoofed by package name tricks
		if (isLoadedByUntrustedClassLoader(caller)) {
			classLogger.error("Unauthorized access to {} attempted from untrusted classloader: {}", context,
					caller.getName());
			throw new SecurityException("Unauthorized access to " + context
					+ " from class loaded by untrusted classloader: " + caller.getName());
		}

		// Check 2: package allowlist
		String callerPackage = caller.getPackageName();
		boolean allowed = allowedPackages.stream()
				.anyMatch(p -> callerPackage.equals(p) || callerPackage.startsWith(p + "."));

		if (!allowed) {
			classLogger.error("Unauthorized access to {} attempted from: {}", context, caller.getName());
			throw new SecurityException("Unauthorized access to " + context + " from: " + caller.getName());
		}
	}

	/**
	 * Returns true if the class was loaded by any user-defined classloader rather
	 * than one of the JVM's two built-in trusted loaders (platform or system/app).
	 *
	 * Checking the loader directly (rather than walking its parent chain) catches
	 * every custom loader, SemossClassLoader, URLClassLoader wrapping it,
	 * xeustechnologies JarClassLoader used for Maven reactor dependencies, and any
	 * future custom loader, without needing to enumerate them by type.
	 *
	 * Bootstrap-loaded classes have a null classloader and are always trusted.
	 * 
	 * @param clazz
	 * @return
	 */
	private static boolean isLoadedByUntrustedClassLoader(Class<?> clazz) {
		ClassLoader loader = clazz.getClassLoader();
		return loader != null && loader != PLATFORM_CL && loader != SYSTEM_CL && loader != WEBAPP_CL;
	}

}
