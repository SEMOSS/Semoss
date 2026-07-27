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
package prerna.reactor.agent;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.reactor.agent.config.AgentConfig;
import prerna.reactor.agent.config.AgentConfigLoader;
import prerna.reactor.agent.sandbox.EnforcementMode;
import prerna.reactor.agent.sandbox.SandboxPolicy;
import prerna.reactor.agent.sandbox.SandboxPolicyBuilder;
import prerna.reactor.agent.skill.SkillStager;
import prerna.reactor.agent.subagent.AgentSubAgentRegistry;
import prerna.util.AssetUtility;
import prerna.util.Utility;

/**
 * High-level entry point for resolving context and executing an agent harness.
 */
public final class AgentRunner {

	private static final Logger logger = LogManager.getLogger(AgentRunner.class);

	/**
	 * Key under which the resolved working directory is injected into
	 * {@code paramMap}.
	 */
	public static final String FILE_PATH_PARAM_KEY = "file_path";

	/**
	 * paramMap key for an explicit workspace id (agent identity) that overrides
	 * whatever {@code room.options.workspace.workspace_id} carries. Stripped from
	 * paramMap by {@link #extractExplicitWorkspaceId(Map)} before the model engine
	 * call.
	 */
	public static final String PARAM_WORKSPACE_ID = "workspace_id";
	private static final String PARAM_WORKSPACE_ID_CAMEL = "workspaceId";

	/**
	 * paramMap key: target SEMOSS project (workspace) the agent should operate
	 * inside. Resolves to that project's {@code assets/} folder. Mutually preferred
	 * over the default (current room's folder).
	 */
	public static final String PARAM_PROJECT = "project";

	/**
	 * paramMap key: relative subfolder inside the resolved container (room or
	 * project). Must be relative (no leading {@code /}, {@code \}, or {@code ~})
	 * and must not contain {@code ..} segments. The resolved path is
	 * canonical-checked to stay under the container.
	 */
	public static final String PARAM_SUBDIR = "subdir";

	/**
	 * paramMap key: legacy absolute working-dir path. <b>Deprecated.</b> Callers
	 * should use {@link #PARAM_PROJECT} + {@link #PARAM_SUBDIR}. Logged + ignored.
	 */
	public static final String PARAM_FILE_PATH_LEGACY = "filePath";

	/**
	 * Room option for an absolute working-dir override.
	 *
	 * <p>
	 * Used mainly by spawned child rooms that should operate inside the parent's
	 * workdir. The resolved path must stay under {@code Utility.getBaseFolder()}.
	 */
	public static final String ROOM_OPTION_WORKING_DIR = "working_dir";

	/** Options-map keys checked (in order) when room.getModelId() is not set. */
	private static final String[] MODEL_ID_OPTION_KEYS = { "engine", "model", "modelId", "engineId" };

	// paramMap keys that let the caller extend the default sandbox policy.
	/** List of absolute paths to add as read-only to the sandbox policy. */
	public static final String PARAM_SANDBOX_READS = "sandbox_reads";
	/** List of absolute paths to add as read-write to the sandbox policy. */
	public static final String PARAM_SANDBOX_WRITES = "sandbox_writes";
	/** Override enforcement mode per-run: {@code ENFORCE} | {@code DISABLED}. */
	public static final String PARAM_SANDBOX_ENFORCE = "sandbox_enforce";

	private static final Set<String> ACTIVE_ROOMS = ConcurrentHashMap.newKeySet();

	private AgentRunner() {
		/* static utility */ }

	/**
	 * Run the agent loop.
	 *
	 * @param roomId           Required. ROOM table ID that provides model, history,
	 *                         and tools.
	 * @param input            Required. Initial user message.
	 * @param engineIdFallback Optional. Engine/model ID to use if the room has no
	 *                         MODEL_ID set.
	 * @param harnessType      Optional. Registry key for the harness; defaults to
	 *                         {@code "semoss"}.
	 * @param maxTurns         Optional. Maximum SEMOSS harness tool-call rounds.
	 * @param maxReflections   Optional. Maximum SEMOSS harness self-critique
	 *                         rounds.
	 * @param paramMap         Optional. Extra model parameters (temperature,
	 *                         max_tokens, etc.).
	 * @param mediaInputPaths  Optional room-local media filenames for the initial
	 *                         user message.
	 * @param mediaUrls        Optional direct media URLs for the initial user
	 *                         message.
	 * @param runId            Optional durable AGENT_RUN id used to tag room
	 *                         messages.
	 * @param insight          Required. Current insight context (user, project,
	 *                         etc.).
	 * @return Rich result containing final text, iteration count, and tool-call
	 *         trace.
	 * @throws Exception on unrecoverable errors during execution.
	 */
	public static AgentHarnessResult run(String roomId, String input, String engineIdFallback, String harnessType,
			int maxTurns, int maxReflections, Map<String, Object> paramMap, Map<String, Object> agentParamMap,
			List<String> mediaInputPaths, List<String> mediaUrls, String runId, Insight insight) throws Exception {
		return run(roomId, input, engineIdFallback, harnessType, maxTurns, maxReflections, paramMap, agentParamMap,
				mediaInputPaths, mediaUrls, runId, insight, false);
	}

	/**
	 * Overload that accepts a {@code resumeMode} flag. When {@code true}, the
	 * harness skips the initial model ask and continues from the room's latest
	 * message - used when resuming a run that was paused on
	 * {@code SMSS_MCP_EXECUTION=ask} tools.
	 */
	public static AgentHarnessResult run(String roomId, String input, String engineIdFallback, String harnessType,
			int maxTurns, int maxReflections, Map<String, Object> paramMap, Map<String, Object> agentParamMap,
			List<String> mediaInputPaths, List<String> mediaUrls, String runId, Insight insight,
			boolean resumeMode) throws Exception {

		if (roomId == null || roomId.trim().isEmpty()) {
			throw new IllegalArgumentException("roomId is required");
		}
		if (input == null || input.trim().isEmpty()) {
			throw new IllegalArgumentException("input is required");
		}
		if (!ACTIVE_ROOMS.add(roomId)) {
			throw new IllegalStateException("Agent run already in progress for room: " + roomId);
		}
		try {

			IModelEngine modelEngine = null;
			String runtimeModelId = engineIdFallback != null ? engineIdFallback.trim() : null;
			if (runtimeModelId != null && !runtimeModelId.isEmpty()) {
				modelEngine = Utility.getModel(runtimeModelId);
				if (modelEngine == null) {
					throw new IllegalArgumentException(
							"Could not load model engine '" + runtimeModelId + "' for room '" + roomId + "'");
				}
			}

			Room room = modelEngine != null ? RoomUtils.createRoomIfNotExists(roomId, insight, modelEngine, input)
					: RoomUtils.getOrLoadRoom(roomId, insight);

			Map<String, Object> params = paramMap != null ? new HashMap<>(paramMap) : new HashMap<>();
			Map<String, Object> agentParams = agentParamMap != null ? new HashMap<>(agentParamMap) : new HashMap<>();

			// Resolve and strip any per-run workspace override before model + working-dir lookup.
			String explicitWorkspaceId = extractExplicitWorkspaceId(params);
			String effectiveWorkspaceId = explicitWorkspaceId != null ? explicitWorkspaceId
					: extractWorkspaceIdFromOptionField(room.getOptionsMap().get("workspace"));

			String modelId = resolveModelId(room, runtimeModelId, effectiveWorkspaceId);
			if (modelId == null || modelId.trim().isEmpty()) {
				throw new IllegalArgumentException("No model engine found for room '" + roomId + "'. "
						+ "Set MODEL_ID on the room, pass engine= to the reactor, or set model_id on the "
						+ "workspace/agent config (CONFIG_JSON).");
			}
			logger.debug("AgentRunner: room={} resolved modelId={}", roomId, modelId);

			if (modelEngine == null || !modelId.equals(modelEngine.getEngineId())) {
				modelEngine = Utility.getModel(modelId);
			}
			if (modelEngine == null) {
				throw new IllegalArgumentException(
						"Could not load model engine '" + modelId + "' for room '" + roomId + "'");
			}
			room.setModelId(modelId);

			insight.setRoomForInsight(room);

			String filePath = resolveWorkingDir(room, params, effectiveWorkspaceId);
			if (filePath != null && !filePath.trim().isEmpty()) {
				params.put(FILE_PATH_PARAM_KEY, filePath);
			}

			SandboxPolicy sandboxPolicy = buildSandboxPolicyFromParams(params);

			// Resolve the shared agent config once for all harnesses.
			AgentConfig agentConfig = AgentConfigLoader.load(room, filePath, modelId, params, agentParams, maxTurns,
					maxReflections, explicitWorkspaceId);

			try {
				SkillStager.stage(filePath, agentConfig.getSkills());
			} catch (Exception e) {
				logger.warn("AgentRunner: skill staging failed for room='{}': {}", roomId, e.getMessage(), e);
			}

			AgentRunContext ctx = AgentRunContext.builder().room(room).modelEngine(modelEngine).insight(insight)
					.userId(room.getUserId()).input(input).runId(runId).sandboxPolicy(sandboxPolicy)
					.mediaInputPaths(mediaInputPaths).mediaUrls(mediaUrls)
					// Root runs are not registered as subagents and resolve to depth 0.
					// Child runs are recorded by AgentSubAgentRegistry before their
					// virtual thread starts, so this lookup can classify the run without
					// storing transient spawn state on the durable room options.
					.spawnDepth(resolveSpawnDepth())
					.resumeMode(resumeMode)
					.agentConfig(agentConfig).build();

			IAgentHarness harness = AgentHarnessRegistry.getOrDefault(harnessType);
			logger.info("AgentRunner: using harness '{}' for room={}", harness.getName(), roomId);
			if (hasMediaInput(ctx) && !harness.supportsMediaInput()) {
				throw new IllegalArgumentException("RunAgent media input is not supported for harnessType='"
						+ harness.getName() + "'");
			}

			// Apply a temporary workspace overlay so room-based lookups match AgentConfig.
			List<IAgentRunHook> hooks = ctx.getAgentConfig().getRunHooks();
			WorkspaceOverlay wsOverlay = applyWorkspaceOverlay(room, explicitWorkspaceId);
			AgentHarnessResult result = null;
			try {
				// Lifecycle: onRoomCreation - observation-only, exceptions swallowed
				for (IAgentRunHook h : hooks) {
					try {
						h.onRoomCreation(ctx);
					} catch (Exception hookEx) {
						logger.warn("AgentRunner: onRoomCreation hook {} threw - logging and continuing",
								h.getClass().getSimpleName(), hookEx);
					}
				}
				// Lifecycle: beforeRun - veto point, exceptions abort the run
				for (IAgentRunHook h : hooks) {
					h.beforeRun(ctx);
				}
				// Lifecycle: afterAgentInit - observation-only, exceptions swallowed
				for (IAgentRunHook h : hooks) {
					try {
						h.afterAgentInit(ctx);
					} catch (Exception hookEx) {
						logger.warn("AgentRunner: afterAgentInit hook {} threw - logging and continuing",
								h.getClass().getSimpleName(), hookEx);
					}
				}
				result = harness.execute(ctx);
			} finally {
				AgentHarnessResult finalResult = result;
				// Lifecycle: afterRun - observation-only, exceptions swallowed
				for (IAgentRunHook h : hooks) {
					try {
						h.afterRun(ctx, finalResult);
					} catch (Exception hookEx) {
						logger.warn("AgentRunner: afterRun hook {} threw - logging and continuing",
								h.getClass().getSimpleName(), hookEx);
					}
				}
				// Lifecycle: beforeAgentDeInit - last chance before overlay is restored
				for (IAgentRunHook h : hooks) {
					try {
						h.beforeAgentDeInit(ctx, finalResult);
					} catch (Exception hookEx) {
						logger.warn("AgentRunner: beforeAgentDeInit hook {} threw - logging and continuing",
								h.getClass().getSimpleName(), hookEx);
					}
				}
				restoreWorkspaceOverlay(room, wsOverlay);
			}

			if (ClusterUtil.IS_CLUSTER) {
				try {
					ClusterUtil.pushRoom(roomId);
				} catch (Exception e) {
					logger.warn("AgentRunner: post-agent room push to cloud failed for room='{}'", roomId, e);
				}
			}

			return result;
		} finally {
			ACTIVE_ROOMS.remove(roomId);
		}
	}

	public static AgentHarnessResult run(String roomId, String input, String engineIdFallback, String harnessType,
			int maxTurns, int maxReflections, Map<String, Object> paramMap, Map<String, Object> agentParamMap,
			String runId, Insight insight) throws Exception {
		return run(roomId, input, engineIdFallback, harnessType, maxTurns, maxReflections, paramMap, agentParamMap,
				null, null, runId, insight);
	}

	// workspace_id overlay helpers

	/**
	 * Captured state needed to restore {@code room.options.workspace} to its
	 * pre-overlay shape. Returned from {@link #applyWorkspaceOverlay} and consumed
	 * by {@link #restoreWorkspaceOverlay}. Immutable.
	 */
	private static final class WorkspaceOverlay {
		private final Room room;
		private final boolean hadField;
		private final Object originalWorkspace;

		WorkspaceOverlay(Room room, boolean hadField, Object originalWorkspace) {
			this.room = room;
			this.hadField = hadField;
			this.originalWorkspace = originalWorkspace;
		}
	}

	/**
	 * Applies an in-memory workspace override for one run when needed.
	 */
	private static WorkspaceOverlay applyWorkspaceOverlay(Room room, String explicitWorkspaceId) {
		if (explicitWorkspaceId == null || explicitWorkspaceId.trim().isEmpty()) {
			return null;
		}
		Map<String, Object> opts = room.getOptionsMap();
		boolean hadField = opts.containsKey("workspace");
		Object originalWorkspace = opts.get("workspace");

		// No-op when the room already points at this workspace_id.
		String currentId = extractWorkspaceIdFromOptionField(originalWorkspace);
		if (explicitWorkspaceId.equals(currentId)) {
			return null;
		}

		Map<String, Object> newWorkspace = new HashMap<>();
		newWorkspace.put("workspace_id", explicitWorkspaceId);
		// Best-effort name lookup keeps the options shape familiar to callers.
		try {
			Map<String, Object> ws = ModelInferenceLogsUtils.getWorkspaceEntry(explicitWorkspaceId);
			if (ws != null && ws.get("name") != null) {
				newWorkspace.put("name", String.valueOf(ws.get("name")));
			}
		} catch (Exception ignore) {
			// best-effort; absence of name doesn't affect resolution
		}
		opts.put("workspace", newWorkspace);
		room.setOptionsMap(opts);

		logger.info("AgentRunner: workspace overlay applied (explicit='{}' for run; original={})", explicitWorkspaceId,
				currentId == null ? "<unset>" : currentId);
		return new WorkspaceOverlay(room, hadField, originalWorkspace);
	}

	/**
	 * Restore the room's {@code options.workspace} field to the value captured
	 * before the overlay was applied. No-op when {@code overlay} is null.
	 */
	private static void restoreWorkspaceOverlay(Room room, WorkspaceOverlay overlay) {
		if (overlay == null) {
			return;
		}
		Map<String, Object> opts = overlay.room.getOptionsMap();
		if (overlay.hadField) {
			opts.put("workspace", overlay.originalWorkspace);
		} else {
			opts.remove("workspace");
		}
		overlay.room.setOptionsMap(opts);
		logger.debug("AgentRunner: workspace overlay restored");
	}

	/**
	 * Extract the {@code workspace_id} from an {@code options.workspace} field
	 * which may be (a) absent, (b) a primitive id string, or (c) a
	 * {@code {workspace_id, name}} map.
	 */
	@SuppressWarnings("unchecked")
	private static String extractWorkspaceIdFromOptionField(Object workspaceField) {
		if (workspaceField == null) {
			return null;
		}
		if (workspaceField instanceof String) {
			String s = ((String) workspaceField).trim();
			return s.isEmpty() ? null : s;
		}
		if (workspaceField instanceof Map) {
			Object id = ((Map<String, Object>) workspaceField).get("workspace_id");
			if (id == null) {
				return null;
			}
			String s = String.valueOf(id).trim();
			return s.isEmpty() ? null : s;
		}
		return null;
	}

	/**
	 * Extract and strip workspace override keys from {@code params}. Returns
	 * {@code null} when absent or blank.
	 */
	private static String extractExplicitWorkspaceId(Map<String, Object> params) {
		Object raw = params.remove(PARAM_WORKSPACE_ID);
		Object camelRaw = params.remove(PARAM_WORKSPACE_ID_CAMEL);
		if (raw == null) {
			raw = camelRaw;
		}
		if (raw == null) {
			return null;
		}
		String s = String.valueOf(raw).trim();
		return s.isEmpty() ? null : s;
	}

	/**
	 * Resolve this run's position in the subagent tree from the current async job
	 * id.
	 *
	 * <p>
	 * The current policy is root-only spawning: a normal {@code RunAgent} call has
	 * no {@link prerna.reactor.agent.subagent.SubAgentMeta} entry and resolves to
	 * depth 0, while spawned child jobs resolve to the depth recorded by
	 * {@link AgentSubAgentRegistry#spawn}. The harness uses this value to decide
	 * whether to expose spawn tools.
	 */
	private static int resolveSpawnDepth() {
		String jobId = ThreadStore.getJobId();
		if (jobId == null || jobId.isBlank()) {
			return AgentRunContext.ROOT_SPAWN_DEPTH;
		}
		return AgentSubAgentRegistry.getManager().getDepthForJob(jobId);
	}

	private static boolean hasMediaInput(AgentRunContext ctx) {
		return ctx != null && (!ctx.getMediaInputPaths().isEmpty() || !ctx.getMediaUrls().isEmpty());
	}

	/**
	 * Resolves the working directory from room state plus {@code project} and
	 * {@code subdir} params.
	 *
	 * <p>
	 * {@code filePath} is deprecated and ignored. {@code project} stays in the
	 * param map because downstream hooks still read it.
	 *
	 * @param effectiveWorkspaceId resolved workspace id (explicit override or
	 *                             {@code room.options.workspace.workspace_id});
	 *                             {@code null} when the room has no workspace
	 *                             binding. Used only for the
	 *                             {@code CONFIG_JSON.subdir} fallback.
	 *
	 * @throws IllegalArgumentException for unresolvable project, illegal subdir, or
	 *                                  containment failure
	 */
	private static String resolveWorkingDir(Room room, Map<String, Object> params, String effectiveWorkspaceId) {
		// Legacy filePath - strip + warn, never honor.
		Object legacyFilePath = params.remove(PARAM_FILE_PATH_LEGACY);
		if (legacyFilePath != null && !String.valueOf(legacyFilePath).trim().isEmpty()) {
			logger.warn("AgentRunner: '{}' is deprecated and ignored - use '{}' + '{}' instead. value='{}'",
					PARAM_FILE_PATH_LEGACY, PARAM_PROJECT, PARAM_SUBDIR, legacyFilePath);
		}

		// Room-level override wins when present, mainly for inherited subagent runs.
		Object roomLevelOverride = room.getOptionsMap() == null ? null
				: room.getOptionsMap().get(ROOM_OPTION_WORKING_DIR);
		if (roomLevelOverride != null) {
			String raw = String.valueOf(roomLevelOverride).trim();
			if (!raw.isEmpty()) {
				String canonical;
				try {
					canonical = new File(raw).getCanonicalPath();
				} catch (IOException ioe) {
					throw new IllegalArgumentException("AgentRunner: room.options." + ROOM_OPTION_WORKING_DIR
							+ " could not be canonicalized: " + raw);
				}
				String baseCanonical;
				try {
					baseCanonical = new File(Utility.getBaseFolder()).getCanonicalPath();
				} catch (IOException ioe) {
					throw new IllegalStateException("AgentRunner: could not canonicalize SEMOSS base folder", ioe);
				}
				if (!canonical.equals(baseCanonical) && !canonical.startsWith(baseCanonical + File.separator)) {
					throw new IllegalArgumentException("AgentRunner: room.options." + ROOM_OPTION_WORKING_DIR
							+ " must be under SEMOSS base folder. value='" + raw + "' resolvedTo='" + canonical
							+ "' baseFolder='" + baseCanonical + "'");
				}
				File f = new File(canonical);
				if (!f.exists()) {
					f.mkdirs();
				}
				logger.info("AgentRunner: working dir overridden by room.options.{}='{}' (room={})",
						ROOM_OPTION_WORKING_DIR, canonical, room.getId());
				// Subdir is intentionally ignored when an absolute override is supplied -
				// the room option already names the final path.
				params.remove(PARAM_SUBDIR);
				return canonical;
			}
		}

		// 1. Container.
		// Peek at PARAM_PROJECT (don't remove) - downstream consumers including
		// GitCommitAgentHook.afterRun rely on params["project"] to know
		// which project's git folder to commit against. The model engine
		// treats unknown keys as no-ops, so leaving the project id in the
		// map is safe.
		String container;
		String containerLabel;
		Object projectObj = params.get(PARAM_PROJECT);
		if (projectObj != null && !String.valueOf(projectObj).trim().isEmpty()) {
			String projectId = String.valueOf(projectObj).trim();
			container = AssetUtility.getProjectAssetsFolder(projectId);
			if (container == null || container.trim().isEmpty()) {
				throw new IllegalArgumentException(
						"AgentRunner: could not resolve assets folder for project='" + projectId + "'");
			}
			containerLabel = "project=" + projectId;
			logger.info("AgentRunner: container resolved from project='{}' -> '{}'", projectId, container);
		} else {
			container = room.getRoomFolderPath();
			File roomFolder = new File(container);
			if (!roomFolder.exists()) {
				roomFolder.mkdirs();
			}
			containerLabel = "room=" + room.getId();
			logger.info("AgentRunner: container defaulted to room folder='{}' (room={})", container, room.getId());
		}

		// 2. Subdir: paramMap override first, then CONFIG_JSON.subdir for the
		// workspace.
		Object subdirObj = params.remove(PARAM_SUBDIR);
		String subdir = subdirObj == null ? null : String.valueOf(subdirObj).trim();
		if (subdir == null || subdir.isEmpty()) {
			subdir = resolveSubdirFromConfigJson(effectiveWorkspaceId);
			if (subdir != null) {
				logger.info("AgentRunner: subdir='{}' resolved from CONFIG_JSON for workspaceId='{}'", subdir,
						effectiveWorkspaceId);
			}
		}
		if (subdir == null || subdir.isEmpty()) {
			return container;
		}
		return joinSubdir(container, subdir, containerLabel);
	}

	/**
	 * Pull {@code CONFIG_JSON.subdir} for the given workspace, or {@code null} when
	 * the workspace has no row / no CONFIG_JSON / no {@code subdir} key. Errors are
	 * logged warn and treated as null so a CONFIG_JSON outage never blocks a run.
	 */
	private static String resolveSubdirFromConfigJson(String workspaceId) {
		if (workspaceId == null || workspaceId.trim().isEmpty()) {
			return null;
		}
		try {
			org.json.JSONObject cfg = ModelInferenceLogsUtils.getWorkspaceConfigJson(workspaceId);
			if (cfg == null) {
				return null;
			}
			String v = cfg.optString("subdir", null);
			if (v == null) {
				return null;
			}
			v = v.trim();
			return v.isEmpty() ? null : v;
		} catch (Exception e) {
			logger.warn("AgentRunner: CONFIG_JSON.subdir read failed for workspaceId='{}': {}", workspaceId,
					e.getMessage());
			return null;
		}
	}

	/**
	 * Join {@code subdir} under {@code container}, validating that the result stays
	 * inside the container after canonicalisation. Rejects absolute paths and
	 * {@code ..} escape.
	 */
	private static String joinSubdir(String container, String subdir, String containerLabel) {
		if (subdir.startsWith("/") || subdir.startsWith("\\") || subdir.startsWith("~")) {
			throw new IllegalArgumentException("subdir must be relative (no leading '/', '\\', or '~'); got '" + subdir
					+ "' under " + containerLabel);
		}
		if (subdir.contains("..")) {
			throw new IllegalArgumentException(
					"subdir must not contain '..' segments; got '" + subdir + "' under " + containerLabel);
		}
		File containerFile = new File(container);
		File joined = new File(containerFile, subdir);
		String containerCanonical;
		String joinedCanonical;
		try {
			containerCanonical = containerFile.getCanonicalPath();
			joinedCanonical = joined.getCanonicalPath();
		} catch (IOException e) {
			throw new IllegalArgumentException("Could not canonicalize working dir for " + containerLabel + " + '"
					+ subdir + "': " + e.getMessage(), e);
		}
		// Allow equality (subdir resolves to container itself) or strict-subdir
		// relationship.
		if (!joinedCanonical.equals(containerCanonical)
				&& !joinedCanonical.startsWith(containerCanonical + File.separator)) {
			throw new IllegalArgumentException("subdir '" + subdir + "' escapes container (" + containerLabel + "): "
					+ joinedCanonical + " is outside " + containerCanonical);
		}
		logger.info("AgentRunner: subdir='{}' joined to container -> '{}'", subdir, joinedCanonical);
		return joinedCanonical;
	}

	/**
	 * Build a {@link SandboxPolicy} from pixel-level overrides in {@code paramMap}
	 * when any of {@link #PARAM_SANDBOX_READS}, {@link #PARAM_SANDBOX_WRITES}, or
	 * {@link #PARAM_SANDBOX_ENFORCE} is present. Consumed keys are removed so they
	 * don't bleed into model engine params.
	 *
	 * <p>
	 * Returns {@code null} when no overrides are supplied; harnesses will then
	 * build a DIHelper-backed default via
	 * {@code AgentSandboxConfig.defaultPolicy(...)}.
	 */
	@SuppressWarnings("unchecked")
	private static SandboxPolicy buildSandboxPolicyFromParams(Map<String, Object> params) {
		Object readsObj = params.remove(PARAM_SANDBOX_READS);
		Object writesObj = params.remove(PARAM_SANDBOX_WRITES);
		Object enforceObj = params.remove(PARAM_SANDBOX_ENFORCE);

		if (readsObj == null && writesObj == null && enforceObj == null) {
			return null;
		}

		SandboxPolicyBuilder b = SandboxPolicy.builder();
		if (readsObj instanceof java.util.List) {
			for (Object p : (java.util.List<Object>) readsObj) {
				if (p != null) {
					b.withRead(String.valueOf(p));
				}
			}
		}
		if (writesObj instanceof java.util.List) {
			for (Object p : (java.util.List<Object>) writesObj) {
				if (p != null) {
					b.withReadWrite(String.valueOf(p));
				}
			}
		}
		if (enforceObj instanceof String) {
			try {
				b.withEnforcement(EnforcementMode.valueOf(((String) enforceObj).trim().toUpperCase()));
			} catch (IllegalArgumentException e) {
				logger.warn("Invalid sandbox_enforce value '{}' - keeping default", enforceObj);
			}
		}
		return b.build();
	}

	/**
	 * Resolves the model/engine ID using a four-tier priority:
	 *
	 * <ol>
	 * <li>Runtime override - {@code engine=} passed explicitly to
	 * {@code RunAgent()}.
	 * <li>Room {@code MODEL_ID} column - the room's bound engine.
	 * <li>Room {@code options} map - legacy keys ({@code engine}, {@code model},
	 * etc.) used by older rooms that stored the model id in options rather than the
	 * column.
	 * <li>Workspace/agent config - {@code WORKSPACE.CONFIG_JSON.model_id}, the
	 * per-agent default that lets callers run an agent without binding a model to
	 * the room or passing {@code engine=}.
	 * </ol>
	 *
	 * @param workspaceId effective workspace id for this run (may be null); used to
	 *                    look up the CONFIG_JSON default in tier 4.
	 */
	@SuppressWarnings("unchecked")
	private static String resolveModelId(Room room, String runtimeOverride, String workspaceId) {
		// Tier 1: explicit runtime engine= param wins.
		if (runtimeOverride != null && !runtimeOverride.trim().isEmpty()) {
			logger.info("AgentRunner: using runtime engine override={}", runtimeOverride);
			return runtimeOverride.trim();
		}

		// Tier 2: room's bound MODEL_ID column.
		String modelId = room.getModelId();
		if (modelId != null && !modelId.trim().isEmpty()) {
			return modelId.trim();
		}

		// Tier 3: legacy options map (older rooms stored it under "engine" / "model" /
		// etc.)
		Map<String, Object> opts = room.getOptionsMap();
		if (opts != null) {
			for (String key : MODEL_ID_OPTION_KEYS) {
				Object val = opts.get(key);
				if (val instanceof String && !((String) val).trim().isEmpty()) {
					logger.info("AgentRunner: resolved modelId from options['{}']={}", key, val);
					return ((String) val).trim();
				}
			}
		}

		// Tier 4: workspace/agent CONFIG_JSON default model. Best-effort - a missing
		// workspace, empty column, or parse failure just falls through to null.
		if (workspaceId != null && !workspaceId.trim().isEmpty()) {
			try {
				JSONObject cfg = ModelInferenceLogsUtils.getWorkspaceConfigJson(workspaceId.trim());
				if (cfg != null) {
					String cfgModelId = cfg.optString("model_id", null);
					if (cfgModelId != null && !cfgModelId.trim().isEmpty()) {
						logger.info("AgentRunner: resolved modelId from CONFIG_JSON.model_id={} (workspace={})",
								cfgModelId, workspaceId);
						return cfgModelId.trim();
					}
				}
			} catch (Exception e) {
				logger.warn("AgentRunner: CONFIG_JSON model_id lookup failed for workspace '{}': {}", workspaceId,
						e.getMessage());
			}
		}

		return null;
	}
}
