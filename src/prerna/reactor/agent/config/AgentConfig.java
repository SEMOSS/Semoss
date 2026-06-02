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
package prerna.reactor.agent.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.reactor.agent.IAgentRunHook;
import prerna.reactor.agent.IToolHook;

/**
 * Immutable resolved agent configuration for one run.
 *
 * <p>Built once by {@link AgentConfigLoader} and shared across harnesses. It
 * carries identity, prompt layers, model settings, working dir, MCP refs,
 * skills, budgets, hooks, and subagent specs.
 */
public final class AgentConfig {

    /** JSON schema version of this object. Starts at 1; bumps independently from {@code WORKSPACE.CONFIG_VERSION}. */
    public static final int SCHEMA_VERSION = 1;

    // Identity
    private final String workspaceId;
    private final String name;
    private final String description;

    // Prompt layers
    private final String authoredPrompt;
    private final String agentAgentsMd;
    private final String workdirAgentsMd;

    // Model
    private final String modelId;
    private final Map<String, Object> modelParams;
   
    // Agent specific params
    private final Map<String, Object> agentParams;

    // Filesystem
    private final String workingDir;

    // MCP tool projects (union of WORKSPACE_RESOURCE + room.options.mcp[])
    private final List<Map<String, String>> mcps;

    // Skill refs (union of WORKSPACE_RESOURCE['SKILL'] + CONFIG_JSON.skills[] + room.options.skills[])
    private final List<Map<String, String>> skills;

    // Budgets (nested)
    private final Budgets budgets;

    // Subagent-spawn policy (nested) depth + spawn-count caps
    private final SubAgentSpawnPolicy spawnPolicy;

    // Run hooks (resolved instances; never null, empty when none configured)
    private final List<IAgentRunHook> runHooks;

    // Tool hooks (resolved instances; never null, empty when none configured)
    private final List<IToolHook> toolHooks;

    // Named subagent specs (resolved from CONFIG_JSON.subagents[]; never null, empty when none configured).
    // Semoss harness synthesizes one MCP tool per spec; CLI harnesses ignore this field.
    private final List<SubAgentSpec> subagents;

    AgentConfig(Builder b) {
        this.workspaceId     = b.workspaceId;
        this.name            = b.name;
        this.description     = b.description;
        this.authoredPrompt  = b.authoredPrompt;
        this.agentAgentsMd   = b.agentAgentsMd;
        this.workdirAgentsMd = b.workdirAgentsMd;
        this.modelId         = b.modelId;
        this.modelParams     = b.modelParams != null
                ? Collections.unmodifiableMap(new HashMap<>(b.modelParams))
                : Collections.emptyMap();
        this.agentParams     = b.agentParams != null
        		? Collections.unmodifiableMap(new HashMap<>(b.agentParams))
        		: Collections.emptyMap();
        this.workingDir      = b.workingDir;
        this.mcps            = b.mcps != null
                ? Collections.unmodifiableList(new ArrayList<>(b.mcps))
                : Collections.emptyList();
        this.skills          = b.skills != null
                ? Collections.unmodifiableList(new ArrayList<>(b.skills))
                : Collections.emptyList();
        this.budgets         = b.budgets != null ? b.budgets : Budgets.defaults();
        this.spawnPolicy     = b.spawnPolicy != null ? b.spawnPolicy : SubAgentSpawnPolicy.defaults();
        this.runHooks        = b.runHooks != null
                ? Collections.unmodifiableList(new ArrayList<>(b.runHooks))
                : Collections.emptyList();
        this.toolHooks       = b.toolHooks != null
                ? Collections.unmodifiableList(new ArrayList<>(b.toolHooks))
                : Collections.emptyList();
        this.subagents       = b.subagents != null
                ? Collections.unmodifiableList(new ArrayList<>(b.subagents))
                : Collections.emptyList();
    }

    // Identity
    /** Workspace id ({@code workspace.workspace_id}); {@code null} for ad-hoc rooms with no workspace binding. */
    public String getWorkspaceId() {
        return workspaceId;
    }

    /** Workspace display name; {@code null} when not bound. */
    public String getName() {
        return name;
    }

    /** Workspace description; {@code null} when not set. */
    public String getDescription() {
        return description;
    }

    // Prompt layers
    /**
     * The authored system prompt: {@code room.options.instructions} when set,
     * otherwise {@code workspace.system_prompt}. {@code null} when neither layer is set.
     */
    public String getAuthoredPrompt() {
        return authoredPrompt;
    }

    /**
     * Contents of the agent's own {@code AGENTS.md} / {@code CLAUDE.md}, loaded from
     * the workspace's project assets folder. {@code null} when not present or not yet wired.
     */
    public String getAgentAgentsMd() {
        return agentAgentsMd;
    }

    /**
     * Contents of {@code AGENTS.md} / {@code CLAUDE.md} discovered by walking up from
     * the working directory. {@code null} when none found.
     */
    public String getWorkdirAgentsMd() {
        return workdirAgentsMd;
    }

    /**
     * Convenience: the three agent-side layers joined with blank-line separators.
     * Returns an empty string (never {@code null}) when no layer is populated.
     */
    public String getComposedAgentPrompt() {
        StringBuilder sb = new StringBuilder();
        appendLayer(sb, agentAgentsMd);
        appendLayer(sb, workdirAgentsMd);
        appendLayer(sb, authoredPrompt);
        return sb.toString();
    }

    private static void appendLayer(StringBuilder sb, String layer) {
        if (layer == null || layer.isEmpty()) {
            return;
        }
        if (sb.length() > 0) {
            sb.append("\n\n");
        }
        sb.append(layer);
    }

    // Model
    /** Model engine id the agent should run against; {@code null} when not yet resolved. */
    public String getModelId() {
        return modelId;
    }

    /**
     * Parameters forwarded to the model engine (temperature, max_tokens, ...).
     * Never {@code null}; always an unmodifiable view.
     *
     * <p>In v1, this is the {@code paramMap} originally passed to
     * {@code AgentRunner.run(...)} - harnesses may still strip harness-only keys
     * before sending to the provider. Future loader work will canonicalize these.
     */
    public Map<String, Object> getModelParams() {
        return modelParams;
    }
    
    /**
     * Parameters for the agent.. Useful for passing parameters to hooks
     * Never {@code null}; always an unmodifiable view.
     */
    public Map<String, Object> getAgentParams() {
    	return agentParams;
    }

    // Filesystem
    /**
     * Working directory the agent operates in (the project being worked on).
     * Used by file/tool resolution and by {@code AGENTS.md} discovery.
     * {@code null} or empty when not set.
     */
    public String getWorkingDir() {
        return workingDir;
    }

    // MCP tool projects
    /**
     * Resolved MCP project refs for this run.
     *
     * <p>Entries come from workspace resources plus any room-level additions.
     * CLI harnesses use these ids to configure external MCP clients, while the
     * in-process SEMOSS harness resolves full tool schemas through {@code Room}.
     */
    public List<Map<String, String>> getMcps() {
        return mcps;
    }

    // Skills
    /**
     * Resolved skill refs for this run.
     *
     * <p>Each entry includes {@code skill_id} and an optional
     * {@code pinned_version}. {@link prerna.reactor.agent.skill.SkillStager}
     * consumes this list to materialize the working copy under
     * {@code .claude/skills/}.
     */
    public List<Map<String, String>> getSkills() {
        return skills;
    }

    // Budgets
    /** Run-time budgets (turn cap, reflection cap, wall-clock). Never {@code null}. */
    public Budgets getBudgets() {
        return budgets;
    }

    // Subagent-spawn policy
    /** Subagent-spawn policy (depth + spawn counts). Never {@code null}. */
    public SubAgentSpawnPolicy getSpawnPolicy() {
        return spawnPolicy;
    }

    // Hooks
    public List<IAgentRunHook> getRunHooks() {
        return runHooks;
    }

    // Tool hooks - fired before/after each tool dispatch inside HarnessToolExecutor.
    public List<IToolHook> getToolHooks() {
        return toolHooks;
    }

    // Subagents
    /**
     * Named subagent specs declared in {@code CONFIG_JSON.subagents[]}.
     *
     * <p>The semoss harness exposes them as synthesized tools. CLI harnesses
     * read but ignore this list.
     */
    public List<SubAgentSpec> getSubagents() {
        return subagents;
    }

    // Builder
    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String workspaceId;
        private String name;
        private String description;
        private String authoredPrompt;
        private String agentAgentsMd;
        private String workdirAgentsMd;
        private String modelId;
        private Map<String, Object> modelParams;
        private Map<String, Object> agentParams;
        private String workingDir;
        private List<Map<String, String>> mcps;
        private List<Map<String, String>> skills;
        private Budgets             budgets;
        private SubAgentSpawnPolicy spawnPolicy;
        private List<IAgentRunHook> runHooks;
        private List<IToolHook>     toolHooks;
        private List<SubAgentSpec>  subagents;

        public Builder workspaceId(String v)         { this.workspaceId = v;         return this; }
        public Builder name(String v)                { this.name = v;                return this; }
        public Builder description(String v)         { this.description = v;         return this; }
        public Builder authoredPrompt(String v)      { this.authoredPrompt = v;      return this; }
        public Builder agentAgentsMd(String v)       { this.agentAgentsMd = v;       return this; }
        public Builder workdirAgentsMd(String v)     { this.workdirAgentsMd = v;     return this; }
        public Builder modelId(String v)             { this.modelId = v;             return this; }
        public Builder modelParams(Map<String, Object> v) { this.modelParams = v;    return this; }
        public Builder agentParams(Map<String, Object> v) { this.agentParams = v;    return this; }
        public Builder workingDir(String v)          { this.workingDir = v;          return this; }
        public Builder mcps(List<Map<String, String>> v) { this.mcps = v;            return this; }
        public Builder skills(List<Map<String, String>> v) { this.skills = v;        return this; }
        public Builder budgets(Budgets v)                       { this.budgets = v;             return this; }
        public Builder spawnPolicy(SubAgentSpawnPolicy v)       { this.spawnPolicy = v;         return this; }
        public Builder runHooks(List<IAgentRunHook> v)          { this.runHooks = v;            return this; }
        public Builder toolHooks(List<IToolHook> v)             { this.toolHooks = v;           return this; }
        public Builder subagents(List<SubAgentSpec> v) { this.subagents = v;         return this; }

        public AgentConfig build() {
            return new AgentConfig(this);
        }
    }

    // Nested: Budgets
    /**
     * Run-time budgets. Immutable; built via {@link #builder()} or
     * {@link #of(int, int, int)}. {@code 0} on any field means "no limit" except
     * {@code maxTurns} which is always positive.
     */
    public static final class Budgets {

        /** Default tool-call round cap. */
        public static final int DEFAULT_MAX_TURNS = 30;
        /** Default reflection cap (0 = no reflection). */
        public static final int DEFAULT_MAX_REFLECTIONS = 0;
        /** Default wall-clock cap (0 = no limit). */
        public static final int DEFAULT_MAX_SECONDS = 0;

        private final int maxTurns;
        private final int maxReflections;
        private final int maxSeconds;

        private Budgets(int maxTurns, int maxReflections, int maxSeconds) {
            if (maxTurns <= 0) {
                throw new IllegalArgumentException("maxTurns must be > 0 (got " + maxTurns + ")");
            }
            if (maxReflections < 0) {
                throw new IllegalArgumentException("maxReflections must be >= 0 (got " + maxReflections + ")");
            }
            if (maxSeconds < 0) {
                throw new IllegalArgumentException("maxSeconds must be >= 0 (got " + maxSeconds + ")");
            }
            this.maxTurns       = maxTurns;
            this.maxReflections = maxReflections;
            this.maxSeconds     = maxSeconds;
        }

        public int getMaxTurns()       { return maxTurns; }
        public int getMaxReflections() { return maxReflections; }
        public int getMaxSeconds()     { return maxSeconds; }

        /** Default budgets: turn cap {@value #DEFAULT_MAX_TURNS}, no reflections, no time limit. */
        public static Budgets defaults() {
            return new Budgets(DEFAULT_MAX_TURNS, DEFAULT_MAX_REFLECTIONS, DEFAULT_MAX_SECONDS);
        }

        public static Budgets of(int maxTurns, int maxReflections, int maxSeconds) {
            return new Budgets(maxTurns, maxReflections, maxSeconds);
        }
    }

    // Nested: SubAgentSpawnPolicy — depth + spawn-count caps for subagent sprawl.
    public static final class SubAgentSpawnPolicy {

        // Default depth cap. 0 disables spawning, 1 = root only, 2 = root + one nested level, etc.
        // Workspaces opt into deeper trees via CONFIG_JSON.spawn_policy.max_subagent_depth.
        public static final int DEFAULT_MAX_SUBAGENT_DEPTH    = 1;
        // Lifetime cap across the whole tree under one root.
        public static final int DEFAULT_MAX_SUBAGENTS_PER_RUN = 10;
        // Per tool batch (one model turn).
        public static final int DEFAULT_MAX_SPAWNS_PER_TURN   = 5;

        private final int maxSubagentDepth;
        private final int maxSubagentsPerRun;
        private final int maxSpawnsPerTurn;

        private SubAgentSpawnPolicy(int maxSubagentDepth, int maxSubagentsPerRun, int maxSpawnsPerTurn) {
            if (maxSubagentDepth < 0) {
                throw new IllegalArgumentException("maxSubagentDepth must be >= 0 (got " + maxSubagentDepth + ")");
            }
            if (maxSubagentsPerRun < 0) {
                throw new IllegalArgumentException("maxSubagentsPerRun must be >= 0 (got " + maxSubagentsPerRun + ")");
            }
            if (maxSpawnsPerTurn < 0) {
                throw new IllegalArgumentException("maxSpawnsPerTurn must be >= 0 (got " + maxSpawnsPerTurn + ")");
            }
            this.maxSubagentDepth   = maxSubagentDepth;
            this.maxSubagentsPerRun = maxSubagentsPerRun;
            this.maxSpawnsPerTurn   = maxSpawnsPerTurn;
        }

        public int getMaxSubagentDepth()    { return maxSubagentDepth; }
        public int getMaxSubagentsPerRun()  { return maxSubagentsPerRun; }
        public int getMaxSpawnsPerTurn()    { return maxSpawnsPerTurn; }

        public static SubAgentSpawnPolicy defaults() {
            return new SubAgentSpawnPolicy(DEFAULT_MAX_SUBAGENT_DEPTH, DEFAULT_MAX_SUBAGENTS_PER_RUN, DEFAULT_MAX_SPAWNS_PER_TURN);
        }

        public static SubAgentSpawnPolicy of(int maxSubagentDepth, int maxSubagentsPerRun, int maxSpawnsPerTurn) {
            return new SubAgentSpawnPolicy(maxSubagentDepth, maxSubagentsPerRun, maxSpawnsPerTurn);
        }
    }
}
