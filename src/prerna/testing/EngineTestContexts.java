package prerna.testing;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.util.SystemDefaultEngines;

/** Short-lived, caller-owned database routing; never changes global engine instances or browser sessions. */
public final class EngineTestContexts {
	public static final String HEADER = "X-Semoss-App-Test";
	public static final String PARAMETER = "__semoss_app_test";
	public static final String WORKER_FILE = "__semoss_app_test_worker.js";
	private static final long TTL_MS = 30 * 60_000L;
	private static final Map<String, Context> CONTEXTS = new ConcurrentHashMap<>();
	private static final ThreadLocal<Binding> CURRENT = new ThreadLocal<>();
	private record Binding(Context context, Insight executionInsight) { }
	public record Context(String token, String owner, String httpSession, String projectId, String configurationId,
			Map<String, String> mappings, User user, long expiresAt) {
		public Map<String, Object> summary() {
			return Map.of("project", projectId, "configurationId", configurationId, "mappings", mappings,
					"expiresAt", Instant.ofEpochMilli(expiresAt).toString());
		}
	}
	public static final class Scope implements AutoCloseable {
		private final Binding prior;
		private Scope(Binding prior) { this.prior = prior; }
		@Override public void close() { if (prior == null) CURRENT.remove(); else CURRENT.set(prior); }
	}
	private EngineTestContexts() { }

	public static String owner(User user) {
		if (user == null || user.getPrimaryLogin() == null || user.getAccessToken(user.getPrimaryLogin()) == null)
			throw new IllegalArgumentException("A signed-in user is required for test database routing");
		return user.getPrimaryLogin().getLabel() + ":" + user.getAccessToken(user.getPrimaryLogin()).getId();
	}

	public static Context begin(User user, String projectId, String httpSession, Map<String, Object> profile) {
		if (httpSession == null || httpSession.isBlank() || !SecurityProjectUtils.userCanEditProject(user, projectId))
			throw new IllegalArgumentException("An authenticated session and app edit access are required");
		if (profile == null) throw new IllegalArgumentException("Configure test databases before starting test database mode");
		EngineTestConfiguration.authorize(user, profile);
		return register(owner(user), httpSession, projectId, String.valueOf(profile.get("id")), EngineTestConfiguration.mappings(profile), user, TTL_MS);
	}

	static synchronized Context register(String owner, String session, String project, String configuration,
			Map<String, String> mappings, User user, long ttl) {
		CONTEXTS.values().removeIf(c -> c.expiresAt <= System.currentTimeMillis());
		if (CONTEXTS.size() >= 256) throw new IllegalArgumentException("Too many active app test contexts");
		Context context = new Context(UUID.randomUUID().toString(), owner, session, project, configuration,
				Map.copyOf(mappings), user, System.currentTimeMillis() + ttl);
		CONTEXTS.put(context.token, context);
		return context;
	}

	private static Context lookup(String token) {
		Context context = CONTEXTS.get(token);
		if (context == null || context.expiresAt <= System.currentTimeMillis())
			throw new IllegalArgumentException("App test context expired or was closed; production fallback is disabled");
		return context;
	}
	public static Context validate(String token, User user, String session) {
		Context context = lookup(token);
		if (!context.owner.equals(owner(user)) || !context.httpSession.equals(session)
				|| !SecurityProjectUtils.userCanEditProject(user, context.projectId))
			throw new IllegalArgumentException("App test context does not belong to this caller/session");
		return context;
	}
	public static Scope enterRequest(String token, User user, String session) {
		return bind(validate(token, user, session), null);
	}
	private static Scope bind(Context context, Insight insight) {
		Binding prior = CURRENT.get();
		if (prior != null && !prior.context.token.equals(context.token))
			throw new IllegalArgumentException("Cannot mix app test contexts");
		CURRENT.set(new Binding(context, insight));
		return new Scope(prior);
	}
	/** A marker captured at Insight construction. It is not itself permission to reuse a live Insight. */
	public static String creationToken() { return currentToken(); }
	public static String currentToken() {
		Binding binding = CURRENT.get();
		return binding == null ? null : lookup(binding.context.token).token;
	}
	public static boolean isActive() { return currentToken() != null; }
	public static Insight executionInsight() {
		Binding binding = CURRENT.get();
		if (binding != null) lookup(binding.context.token);
		return binding == null ? null : binding.executionInsight;
	}

	/** Activate a test Insight on an async/Python callback; expired bindings always fail closed. */
	public static Scope enterInsight(Insight insight, User actingUser) {
		String token = insight.getEngineTestSessionToken();
		String current = currentToken();
		if (token == null && current != null) {
			if (!current.equals(insight.getEngineTestCreationToken()))
				throw new IllegalArgumentException("Test mode requires a fresh Insight; a normal app Insight cannot be reused");
			insight.bindEngineTestSession(current);
			token = current;
		}
		if (token == null) return new Scope(CURRENT.get());
		Context context = lookup(token);
		if (!context.owner.equals(owner(actingUser))) throw new IllegalArgumentException("Test Insight caller changed");
		return bind(context, insight);
	}

	/** Resolve database access and reject all undeclared app databases while a context is active. */
	public static String resolveDatabase(String engineId) {
		Binding binding = CURRENT.get();
		if (binding == null) return engineId;
		Context context = lookup(binding.context.token);
		// Platform metadata databases are not application dependencies and must not be substituted.
		if (SystemDefaultEngines.getIgnoreDatabaseOwlList().contains(engineId)) return engineId;
		String effective = context.mappings.get(engineId);
		if (effective == null && context.mappings.containsValue(engineId)) effective = engineId;
		if (effective == null) throw new IllegalArgumentException("Database is not mapped in EngineTestConfiguration; production access is blocked");
		if (!SecurityEngineUtils.userCanEditEngine(context.user, effective)) throw new IllegalArgumentException("Test database access was revoked");
		return effective;
	}
	public static String resolveEngineArgument(String key, String engineId) {
		if (!isActive() || engineId == null || engineId.isBlank()) return engineId;
		if ("database".equalsIgnoreCase(key)) return resolveDatabase(engineId);
		if ("engine".equalsIgnoreCase(key)
				&& SecurityEngineUtils.getEngineTypeAndSubtype(engineId)[0] == IEngine.CATALOG_TYPE.DATABASE)
			return resolveDatabase(engineId);
		return engineId;
	}
	public static void end(String token, User user, String session) {
		Context context = CONTEXTS.get(token);
		if (context == null) return;
		if (!context.owner.equals(owner(user)) || !context.httpSession.equals(session)) throw new IllegalArgumentException("Cannot close another caller's app test context");
		CONTEXTS.remove(token, context);
	}
	/** End a context already held by its authorized runner. */
	public static void end(Context context) { if (context != null) CONTEXTS.remove(context.token, context); }

	/** Resolve the original executing Insight rather than a shared Python process's global workspace. */
	public static Insight callbackInsight(String globalInsight, String executionInsight) {
		if (executionInsight != null && !executionInsight.isBlank()) {
			Insight insight = InsightStore.getInstance().get(executionInsight);
			if (insight == null) throw new IllegalArgumentException("The execution Insight is no longer available; database access is blocked");
			return insight;
		}
		return globalInsight == null ? null : InsightStore.getInstance().get(globalInsight);
	}

	/** Capture routing explicitly when submitting app work to an existing executor pool. */
	public static Runnable wrap(Runnable task) {
		Binding captured = CURRENT.get();
		return () -> {
			Binding prior = CURRENT.get();
			try {
				if (captured == null) CURRENT.remove();
				else { lookup(captured.context.token); CURRENT.set(captured); }
				task.run();
			} finally { if (prior == null) CURRENT.remove(); else CURRENT.set(prior); }
		};
	}
}
