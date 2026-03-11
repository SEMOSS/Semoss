package prerna.util;

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
	 * classpath and is considered trusted. Anything else — SemossClassloader,
	 * URLClassLoader, JarClassLoader, or any other user-defined loader — is
	 * untrusted regardless of the package name it declares.
	 */
	private static final ClassLoader PLATFORM_CL = ClassLoader.getPlatformClassLoader();
	private static final ClassLoader SYSTEM_CL = ClassLoader.getSystemClassLoader();

	/*
	 * We have a per-engine caller allowlist (prefix based: covers all subpackages)
	 */

	private static final Set<String> SECURITY_DB_ALLOWED = Set.of("prerna.auth", "prerna.reactor.security",
			"prerna.web.conf", "prerna.semoss.web.services.config");

	private static final Set<String> LOCAL_MASTER_DB_ALLOWED = Set.of("prerna.auth", "prerna.masterdatabase",
			"prerna.reactor.masterdatabase", "prerna.reactor.utils", "prerna.util", "prerna.web.conf");

	private static final Set<String> SCHEDULER_DB_ALLOWED = Set.of("prerna.auth", "prerna.reactor.scheduler",
			"prerna.web.conf");

	private static final Set<String> THEMING_DB_ALLOWED = Set.of("prerna.auth", "prerna.theme", "prerna.web.conf");

	private static final Set<String> USER_TRACKING_DB_ALLOWED = Set.of("prerna.auth", "prerna.usertracking",
			"prerna.web.conf");

	private static final Set<String> PROMPT_DB_ALLOWED = Set.of("prerna.auth", "prerna.prompt", "prerna.web.conf");

	private static final Set<String> NOTIFICATION_DB_ALLOWED = Set.of("prerna.auth", "prerna.notifications",
			"prerna.web.conf");

	private static final Set<String> AUDIT_LOGS_DB_ALLOWED = Set.of("prerna.auth", "prerna.engine.logging",
			"prerna.logging", "prerna.web.conf");

	private static final Set<String> MODEL_INFERENCE_LOGS_DB_ALLOWED = Set.of("prerna.auth",
			"prerna.engine.impl.model.inferencetracking", "prerna.web.conf");

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
	// Loaded-state checks — public, no access restriction, callable from anywhere
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

	public static void registerSecurityDb(IRDBMSEngine engine) {
		checkAccess(REGISTRATION_ALLOWED, "SecurityDb registration");
		if (securityDbHolder != null) {
			throw new IllegalStateException("SecurityDb is already registered");
		}
		securityDbHolder = () -> engine;
	}

	public static void registerLocalMasterDb(IRDBMSEngine engine) {
		checkAccess(REGISTRATION_ALLOWED, "LocalMasterDb registration");
		if (localMasterDbHolder != null) {
			throw new IllegalStateException("LocalMasterDb is already registered");
		}
		localMasterDbHolder = () -> engine;
	}

	public static void registerSchedulerDb(IRDBMSEngine engine) {
		checkAccess(REGISTRATION_ALLOWED, "SchedulerDb registration");
		if (schedulerDbHolder != null) {
			throw new IllegalStateException("SchedulerDb is already registered");
		}
		schedulerDbHolder = () -> engine;
	}

	public static void registerThemesDb(IRDBMSEngine engine) {
		checkAccess(REGISTRATION_ALLOWED, "ThemesDb registration");
		if (themesDbHolder != null) {
			throw new IllegalStateException("ThemesDb is already registered");
		}
		themesDbHolder = () -> engine;
	}

	public static void registerUserTrackingDb(IRDBMSEngine engine) {
		checkAccess(REGISTRATION_ALLOWED, "UserTrackingDb registration");
		if (userTrackingDbHolder != null) {
			throw new IllegalStateException("UserTrackingDb is already registered");
		}
		userTrackingDbHolder = () -> engine;
	}

	public static void registerPromptDb(IRDBMSEngine engine) {
		checkAccess(REGISTRATION_ALLOWED, "PromptDb registration");
		if (promptDbHolder != null) {
			throw new IllegalStateException("PromptDb is already registered");
		}
		promptDbHolder = () -> engine;
	}

	public static void registerNotificationDb(IRDBMSEngine engine) {
		checkAccess(REGISTRATION_ALLOWED, "NotificationDb registration");
		if (notificationDbHolder != null) {
			throw new IllegalStateException("NotificationDb is already registered");
		}
		notificationDbHolder = () -> engine;
	}

	public static void registerAuditLogsDb(IRDBMSEngine engine) {
		checkAccess(REGISTRATION_ALLOWED, "AuditLogsDb registration");
		if (auditLogsDbHolder != null) {
			throw new IllegalStateException("AuditLogsDb is already registered");
		}
		auditLogsDbHolder = () -> engine;
	}

	public static void registerModelInferenceLogsDb(IRDBMSEngine engine) {
		checkAccess(REGISTRATION_ALLOWED, "ModelInferenceLogsDb registration");
		if (modelInferenceLogsDbHolder != null) {
			throw new IllegalStateException("ModelInferenceLogsDb is already registered");
		}
		modelInferenceLogsDbHolder = () -> engine;
	}

	/**
	 * Returns true if the given engine ID belongs to one of the known system
	 * engines. Used by Utility.loadEngine() to block the normal load path for these
	 * engines.
	 * 
	 * @param engineId
	 * @return
	 */
	static boolean isSystemEngine(String engineId) {
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
		for (String name : smssProp.stringPropertyNames()) {
			String value = smssProp.getProperty(name);
			if (value != null) {
				smssProp.setProperty(name, value.trim());
			}
		}
		String engineId = smssProp.getProperty(Constants.ENGINE);
		String engineClass = smssProp.getProperty(Constants.ENGINE_TYPE);
		IEngine engine = (IEngine) Class.forName(engineClass).getDeclaredConstructor().newInstance();
		engine.setEngineId(engineId);
		engine.open(smssFilePath);
		IRDBMSEngine rdbmsEngine = (IRDBMSEngine) engine;
		registerSystemEngine(engineId, rdbmsEngine);
		return rdbmsEngine;
	}

	/**
	 * Internal registration switch — maps a system engine ID (via Constants) to
	 * the correct volatile field. Called only from loadSystemEngine.
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
			securityDbHolder = () -> engine;
		}
		case Constants.LOCAL_MASTER_DB -> {
			if (localMasterDbHolder != null) {
				throw new IllegalStateException("LocalMasterDb is already registered");
			}
			localMasterDbHolder = () -> engine;
		}
		case Constants.SCHEDULER_DB -> {
			if (schedulerDbHolder != null) {
				throw new IllegalStateException("SchedulerDb is already registered");
			}
			schedulerDbHolder = () -> engine;
		}
		case Constants.THEMING_DB -> {
			if (themesDbHolder != null) {
				throw new IllegalStateException("ThemesDb is already registered");
			}
			themesDbHolder = () -> engine;
		}
		case Constants.USER_TRACKING_DB -> {
			if (userTrackingDbHolder != null) {
				throw new IllegalStateException("UserTrackingDb is already registered");
			}
			userTrackingDbHolder = () -> engine;
		}
		case Constants.PROMPT_DB -> {
			if (promptDbHolder != null) {
				throw new IllegalStateException("PromptDb is already registered");
			}
			promptDbHolder = () -> engine;
		}
		case Constants.NOTIFICATION_DB -> {
			if (notificationDbHolder != null) {
				throw new IllegalStateException("NotificationDb is already registered");
			}
			notificationDbHolder = () -> engine;
		}
		case Constants.AUDIT_LOGS_DB -> {
			if (auditLogsDbHolder != null) {
				throw new IllegalStateException("AuditLogsDb is already registered");
			}
			auditLogsDbHolder = () -> engine;
		}
		case Constants.MODEL_INFERENCE_LOGS_DB -> {
			if (modelInferenceLogsDbHolder != null) {
				throw new IllegalStateException("ModelInferenceLogsDb is already registered");
			}
			modelInferenceLogsDbHolder = () -> engine;
		}
		default -> throw new IllegalArgumentException("Not a known system engine ID: " + engineId);
		}
	}

	/**
	 * Walks the call stack to find the first frame outside this class, then applies
	 * two independent checks:
	 *
	 * 1. Classloader chain - if the caller's class was loaded by a
	 * SemossClassloader (or any classloader that has one as an ancestor), it is
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
		Class<?> caller = WALKER.walk(frames -> frames.skip(2).map(StackWalker.StackFrame::getDeclaringClass)
				.filter(c -> !c.getName().startsWith("prerna.util.SystemEngineRegistry")).findFirst().orElse(null));

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
	 * every custom loader — SemossClassloader, URLClassLoader wrapping it,
	 * xeustechnologies JarClassLoader used for Maven reactor dependencies, and any
	 * future custom loader — without needing to enumerate them by type.
	 *
	 * Bootstrap-loaded classes have a null classloader and are always trusted.
	 * 
	 * @param clazz
	 * @return
	 */
	private static boolean isLoadedByUntrustedClassLoader(Class<?> clazz) {
		ClassLoader loader = clazz.getClassLoader();
		return loader != null && loader != PLATFORM_CL && loader != SYSTEM_CL;
	}
}
