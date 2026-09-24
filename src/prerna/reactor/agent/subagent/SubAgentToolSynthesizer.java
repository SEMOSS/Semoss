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
package prerna.reactor.agent.subagent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.reactor.agent.config.SubAgentSpec;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.reactor.agent.run.HumanDelegationService;

/**
 * Turns {@link SubAgentSpec} entries + built-in subagent control tools
 * into MCP-style tool definitions the LLM can call.
 *
 * <p>Built-ins always synthesized when any subagent capability is in play:
 * <ul>
 *   <li>{@code SpawnSubAgent} -- anonymous clone of the parent agent</li>
 *   <li>{@code CheckSubAgentStatus} -- non-blocking status peek</li>
 *   <li>{@code WaitForSubAgent}     -- block until a child finishes (or timeout)</li>
 * </ul>
 *
 * <p>Tool dispatch routing uses {@link #BUILTIN_TOOL_NAMES} and the alias set from
 * the loaded {@link SubAgentSpec} list -- names that match are handled by the
 * semoss harness in-process; everything else routes to the normal MCP pipeline.
 */
public final class SubAgentToolSynthesizer {

    /** Built-in subagent control tool names recognized by the dispatcher. */
    public static final String TOOL_SPAWN_SUBAGENT = "SpawnSubAgent";
    public static final String TOOL_CHECK_SUBAGENT = "CheckSubAgentStatus";
    public static final String TOOL_WAIT_SUBAGENT  = "WaitForSubAgent";
    public static final String TOOL_DELEGATE_TO_PERSON = HumanDelegationService.TOOL_NAME;
    /** Convenience set for {@code contains()} checks during dispatch. */
    public static final Set<String> BUILTIN_TOOL_NAMES = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(
                    TOOL_SPAWN_SUBAGENT, TOOL_CHECK_SUBAGENT, TOOL_WAIT_SUBAGENT)));

    private SubAgentToolSynthesizer() {}

    /**
     * Synthesize one MCP tool def per named subagent spec. Order preserved.
     * Returns an empty list when {@code specs} is null or empty.
     */
    public static List<Map<String, Object>> namedTools(List<SubAgentSpec> specs) {
        if (specs == null || specs.isEmpty()) {
            return Collections.emptyList();
        }
        List<Map<String, Object>> out = new ArrayList<>(specs.size());
        for (SubAgentSpec spec : specs) {
            out.add(buildNamedTool(spec));
        }
        return out;
    }

    /**
     * Parent-side built-in subagent control tools. Always returned in the same order:
     * spawn -> check -> wait.
     */
    public static List<Map<String, Object>> builtInTools() {
        List<Map<String, Object>> out = new ArrayList<>(3);
        out.add(buildSpawnTool());
        out.add(buildCheckTool());
        out.add(buildWaitTool());
        return out;
    }

    /**
     * Combined view: named subagent tools followed by the three built-ins.
     * Convenience for harness wiring.
     */
    public static List<Map<String, Object>> allTools(List<SubAgentSpec> specs) {
        List<Map<String, Object>> out = new ArrayList<>();
        out.addAll(namedTools(specs));
        out.addAll(builtInTools());
        return out;
    }

    /**
     * Look up a {@link SubAgentSpec} by alias. {@code null} when {@code toolName}
     * doesn't match any spec alias. Case-sensitive (aliases are user-authored).
     */
    public static SubAgentSpec findSpec(List<SubAgentSpec> specs, String toolName) {
        if (specs == null || toolName == null) return null;
        for (SubAgentSpec spec : specs) {
            if (toolName.equals(spec.getAlias())) {
                return spec;
            }
        }
        return null;
    }

    /** True when {@code toolName} routes to the subagent dispatcher (named or built-in). */
    public static boolean isSubAgentTool(String toolName, List<SubAgentSpec> specs) {
        if (toolName == null) return false;
        if (BUILTIN_TOOL_NAMES.contains(toolName)) return true;
        return findSpec(specs, toolName) != null;
    }

    // Tool def builders
    private static Map<String, Object> buildNamedTool(SubAgentSpec spec) {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("prompt", schemaString(
                "Task, instructions, and all task-specific context or data for the "
                        + spec.getAlias() + " subagent. The named subagent always uses its configured system prompt."));
        properties.put("inherit_parent_workdir", schemaBool(
                "When true, the subagent operates on YOUR resolved working directory (shared filesystem -- any "
                        + "WriteFile/EditFile calls land in YOUR working directory). Stream + history stay isolated. "
                        + "Use this when you want multiple subagents to collaborate on files in one place. "
                        + "Default: false (subagent gets its own private room folder)."));
		properties.put("completionMode", schemaString(
				"WAIT (default): collect the result with WaitForSubAgent. POST: the result is posted to this room "
						+ "when the subagent finishes. POST_AND_CONTINUE: the result is posted, then a new run starts in "
						+ "this room to continue the task. Do not call WaitForSubAgent for POST or POST_AND_CONTINUE."));

        Map<String, Object> inputSchema = new LinkedHashMap<>();
        inputSchema.put("type", "object");
        inputSchema.put("title", spec.getAlias() + "_Arguments");
        inputSchema.put("properties", properties);
        inputSchema.put("required", Collections.singletonList("prompt"));

        String description = spec.getDescription() != null && !spec.getDescription().isBlank()
                ? spec.getDescription() + " (Returns a jobId handle; completionMode says how the result comes back.)"
                : "Delegate to the '" + spec.getAlias() + "' subagent. Returns a jobId handle; completionMode says "
                        + "how the result comes back.";

        Map<String, Object> tool = new LinkedHashMap<>();
        tool.put("name", spec.getAlias());
        tool.put("description", description);
        tool.put("inputSchema", inputSchema);

        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("subagent_alias", spec.getAlias());
        meta.put("subagent_workspace_id", spec.getWorkspaceId());
        meta.put("SMSS_TOOL_KIND", "semoss_subagent_named");
        // Must be explicit or MCPUtility.getValidMcpExecution defaults it to "ask".
        meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.AUTO.getValue());
        tool.put("_meta", meta);

        return tool;
    }

    private static Map<String, Object> buildSpawnTool() {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("prompt", schemaString(
                "Task / instructions for the spawned anonymous subagent."));
        properties.put("context", schemaString(
                "Optional override system prompt for the spawned subagent."));
        properties.put("inherit_parent_workdir", schemaBool(
                "When true, the subagent operates on YOUR resolved working directory (shared filesystem -- any "
                        + "WriteFile/EditFile calls land in YOUR working directory). Stream + history stay isolated. "
                        + "Use this when you want multiple subagents to collaborate on files in one place. "
                        + "Default: false (subagent gets its own private room folder)."));
		properties.put("completionMode", schemaString(
				"WAIT (default): collect the result with WaitForSubAgent. POST: the result is posted to this room "
						+ "when the subagent finishes. POST_AND_CONTINUE: the result is posted, then a new run starts in "
						+ "this room to continue the task. Do not call WaitForSubAgent for POST or POST_AND_CONTINUE."));

        Map<String, Object> inputSchema = new LinkedHashMap<>();
        inputSchema.put("type", "object");
        inputSchema.put("title", "SpawnSubAgent_Arguments");
        inputSchema.put("properties", properties);
        inputSchema.put("required", Collections.singletonList("prompt"));

        Map<String, Object> tool = new LinkedHashMap<>();
        tool.put("name", TOOL_SPAWN_SUBAGENT);
        tool.put("description",
                "Spawn an anonymous subagent (clone of yourself: same model, MCP tools, system prompt). "
                        + "Returns a jobId handle IMMEDIATELY. With the default completionMode=WAIT, call "
                        + "WaitForSubAgent(jobId) to collect the final answer. "
                        + "You may spawn multiple subagents in parallel before waiting on any.");
        tool.put("inputSchema", inputSchema);

        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("SMSS_TOOL_KIND", "semoss_subagent_spawn");
        // See buildNamedTool's comment -- must be explicit or it defaults to "ask".
        meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.AUTO.getValue());
        tool.put("_meta", meta);
        return tool;
    }

    /** Root-run tool that hands a question to a person and returns without waiting. */
    public static Map<String, Object> buildDelegateTool() {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("assignee", schemaString("Name or email of the person to ask, as the user gave it, "
                + "for example \"John Smith\". The user picks the exact person in the card, so do not ask for "
                + "an email first."));
        properties.put("question", schemaString("What you need from them, written so it stands on its own."));
        properties.put("context", schemaString(
                "Everything they need to answer. They cannot see this conversation, your files, or your tools, "
                        + "so include a short summary of only what is relevant and safe to share."));
        properties.put("responseFormat", schemaString("Optional description of the answer you want back."));
        properties.put("dueAt", schemaString("Only when the user gave a deadline: the date as YYYY-MM-DD. "
                + "Leave it out otherwise; do not make one up."));
        properties.put("completionMode", schemaString(
                "POST_AND_CONTINUE (default): their response is posted to this room, then a new run starts to "
                        + "continue the task. POST: the response is only posted."));
        Map<String, Object> files = new LinkedHashMap<>();
        files.put("type", "array");
        files.put("items", Map.of("type", "string"));
        files.put("description", "Optional list of file paths in this room's folder to give them a copy of. "
                + "Plain strings, files only. Example: [\"notes.md\", \"reports/q4.xlsx\"]");
        properties.put("files", files);
        Map<String, Object> linkItem = new LinkedHashMap<>();
        linkItem.put("type", "object");
        linkItem.put("properties", Map.of("url", Map.of("type", "string"), "title", Map.of("type", "string")));
        linkItem.put("required", List.of("url"));
        Map<String, Object> links = new LinkedHashMap<>();
        links.put("type", "array");
        links.put("items", linkItem);
        links.put("description", "Optional links to documents that stay where they are, such as Microsoft 365 or "
                + "SharePoint files. They are not copied; the person needs access at the source.");
        properties.put("links", links);

        Map<String, Object> inputSchema = new LinkedHashMap<>();
        inputSchema.put("type", "object");
        inputSchema.put("title", TOOL_DELEGATE_TO_PERSON + "_Arguments");
        inputSchema.put("properties", properties);
        inputSchema.put("required", Arrays.asList("assignee", "question"));

        Map<String, Object> tool = new LinkedHashMap<>();
        tool.put("name", TOOL_DELEGATE_TO_PERSON);
        tool.put("description",
                "Send a request to another person, by name or email, for their input, review, opinion, or "
                        + "approval. Use it whenever the user asks to send something to someone, share it with them, "
                        + "or get their feedback; it is how you reach people, and there is no separate email tool. "
                        + "Call it right away: the user reviews and can edit the request, files, and links in a card "
                        + "before anything is sent, so do not ask them to confirm first. The person gets their own "
                        + "room to work in and responds when ready, which may take hours or days. Returns once sent; "
                        + "do not wait for or poll the response. Their response arrives in this room as a new "
                        + "message.");
        tool.put("inputSchema", inputSchema);

        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("SMSS_TOOL_KIND", "semoss_delegate_to_person");
        // The user confirms who, what, and which files before anything is shared.
        meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.ASK.getValue());
        meta.put(MCPUtility.SMSS_MCP_UI, new LinkedHashMap<>(
                Map.of(MCPUtility.UI_DISPLAY_LOCATION, MCPUtility.MCPDisplayOption.INLINE.getValue())));
        tool.put("_meta", meta);
        return tool;
    }

    /** Directory lookup by name, email, or username; runs without confirmation. */
    public static Map<String, Object> buildFindPersonTool() {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("query", schemaString("A name, email, or username, full or partial, such as \"John Smith\"."));

        Map<String, Object> inputSchema = new LinkedHashMap<>();
        inputSchema.put("type", "object");
        inputSchema.put("title", HumanDelegationService.FIND_PERSON_TOOL_NAME + "_Arguments");
        inputSchema.put("properties", properties);
        inputSchema.put("required", Collections.singletonList("query"));

        Map<String, Object> tool = new LinkedHashMap<>();
        tool.put("name", HumanDelegationService.FIND_PERSON_TOOL_NAME);
        tool.put("description",
                "Look up people on this platform by name, email, or username. Returns up to 10 matches with name "
                        + "and email. Use it when the user asks who someone is or to check a name. Before "
                        + TOOL_DELEGATE_TO_PERSON + " it is optional: that tool accepts a name and the user picks the "
                        + "exact person in its card.");
        tool.put("inputSchema", inputSchema);

        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("SMSS_TOOL_KIND", "semoss_find_person");
        meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.AUTO.getValue());
        tool.put("_meta", meta);
        return tool;
    }

    /** Delegation-room tool; ask mode makes the person confirm exactly what is sent. */
    public static Map<String, Object> buildSubmitDelegationTool(String requesterName) {
        String requester = requesterName == null ? "the person who asked" : requesterName;
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("response", schemaString(
                "The final answer to send back to " + requester + ", written for them. Required unless declining."));
        properties.put("decline", schemaBool("Set true to decline the request instead of answering it."));
        properties.put("reason", schemaString("Optional reason sent back when declining."));
        Map<String, Object> files = new LinkedHashMap<>();
        files.put("type", "array");
        files.put("items", Map.of("type", "string"));
        files.put("description", "Optional list of file paths in this room's folder to send with the answer, "
                + "including edited copies of files they sent. Plain strings, files only. "
                + "Example: [\"notes.md\", \"from-requester/plan.md\"]");
        properties.put("files", files);

        Map<String, Object> inputSchema = new LinkedHashMap<>();
        inputSchema.put("type", "object");
        inputSchema.put("title", HumanDelegationService.SUBMIT_TOOL_NAME + "_Arguments");
        inputSchema.put("properties", properties);

        Map<String, Object> tool = new LinkedHashMap<>();
        tool.put("name", HumanDelegationService.SUBMIT_TOOL_NAME);
        tool.put("description",
                "Answer the request in this room from " + requester + "; it is the only way to reach them from "
                        + "this room. Call it right away when the user answers the request, or asks to send, reply, "
                        + "or return work to " + requester + ", with the full final text and files. Asking for "
                        + "changes, a draft, or a file is not an answer: do that here and stop. The user confirms in "
                        + "the card, so do not ask them to confirm first. Answering closes the request. Nothing else "
                        + "in this room is shared.");
        tool.put("inputSchema", inputSchema);

        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("SMSS_TOOL_KIND", "semoss_submit_delegation");
        if (requesterName != null) {
            meta.put("SMSS_DELEGATION_REQUESTER", requesterName);
        }
        meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.ASK.getValue());
        // Confirm the send in the chat rather than the side dock.
        meta.put(MCPUtility.SMSS_MCP_UI, new LinkedHashMap<>(
                Map.of(MCPUtility.UI_DISPLAY_LOCATION, MCPUtility.MCPDisplayOption.INLINE.getValue())));
        tool.put("_meta", meta);
        return tool;
    }

    private static Map<String, Object> buildCheckTool() {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("jobId", schemaString(
                "Subagent job id (returned by a spawn or named-subagent tool)."));

        Map<String, Object> inputSchema = new LinkedHashMap<>();
        inputSchema.put("type", "object");
        inputSchema.put("title", "CheckSubAgentStatus_Arguments");
        inputSchema.put("properties", properties);
        inputSchema.put("required", Collections.singletonList("jobId"));

        Map<String, Object> tool = new LinkedHashMap<>();
        tool.put("name", TOOL_CHECK_SUBAGENT);
        tool.put("description",
                "Non-blocking peek at a subagent's job status. Returns the envelope "
                        + "{jobId, status, result, error}. Status values: "
                        + "'running' (still working) or terminal: "
                        + "'succeeded' (result holds final text) / "
                        + "'failed' (error holds short message) / "
                        + "'cancelled'. "
                        + "WHEN status is terminal AND you (or the user) want the actual output, "
                        + "IMMEDIATELY call WaitForSubAgent(jobId) in the same turn -- it returns "
                        + "the same envelope and is the canonical fetch path. Use CheckSubAgentStatus "
                        + "only when you need a non-blocking status; if you already know you want "
                        + "the output, skip check and call WaitForSubAgent directly. "
                        + "When the user asks whether a job is done, report the result explicitly "
                        + "in plain language as the FIRST sentence of your reply (\"No, it is still "
                        + "running\" / \"Yes, it is complete\") -- do not rely on the raw status "
                        + "value or tool-narration to convey completion state.");
        tool.put("inputSchema", inputSchema);

        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("SMSS_TOOL_KIND", "semoss_subagent_check");
        // See buildNamedTool's comment -- must be explicit or it defaults to "ask".
        meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.AUTO.getValue());
        tool.put("_meta", meta);
        return tool;
    }

    private static Map<String, Object> buildWaitTool() {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("jobId", schemaString(
                "Subagent job id (returned by a spawn or named-subagent tool)."));
        Map<String, Object> timeoutProp = new LinkedHashMap<>();
        timeoutProp.put("description",
                "Maximum seconds to wait. On wait-side timeout the envelope reports "
                        + "status='running' (the child keeps running) and the parent may call "
                        + "again. Default 300.");
        timeoutProp.put("title", "timeoutSec");
        timeoutProp.put("type", "number");
        properties.put("timeoutSec", timeoutProp);

        Map<String, Object> inputSchema = new LinkedHashMap<>();
        inputSchema.put("type", "object");
        inputSchema.put("title", "WaitForSubAgent_Arguments");
        inputSchema.put("properties", properties);
        inputSchema.put("required", Collections.singletonList("jobId"));

        Map<String, Object> tool = new LinkedHashMap<>();
        tool.put("name", TOOL_WAIT_SUBAGENT);
        tool.put("description",
                "Block until a spawned subagent reaches a terminal status (or timeoutSec elapses). "
                        + "Always returns the envelope {jobId, status, result, error}. "
                        + "On success: status='succeeded' with the final text in result. "
                        + "On failure: status='failed'/'cancelled' with a short error message. "
                        + "On wait-side timeout: status='running' (the subagent keeps working in the "
                        + "background and you can call WaitForSubAgent again later). "
                        + "Use this whenever you want a subagent's OUTPUT -- either right after spawn "
                        + "(blocking pattern) or after CheckSubAgentStatus says it's done (deferred "
                        + "pattern). If the subagent is already complete, this returns immediately. "
                        + "When the user asks any variant of 'are they done', 'what did they say', "
                        + "'did you finish', or 'collect the results', call WaitForSubAgent -- do not "
                        + "just report status.");
        tool.put("inputSchema", inputSchema);

        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("SMSS_TOOL_KIND", "semoss_subagent_wait");
        // See buildNamedTool's comment -- must be explicit or it defaults to "ask".
        meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.AUTO.getValue());
        tool.put("_meta", meta);
        return tool;
    }

    private static Map<String, Object> schemaString(String description) {
        Map<String, Object> prop = new LinkedHashMap<>();
        prop.put("description", description);
        prop.put("type", "string");
        return prop;
    }

    private static Map<String, Object> schemaBool(String description) {
        Map<String, Object> prop = new LinkedHashMap<>();
        prop.put("description", description);
        prop.put("type", "boolean");
        return prop;
    }
}
