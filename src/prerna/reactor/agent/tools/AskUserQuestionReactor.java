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
package prerna.reactor.agent.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Asks the user a structured question and pauses the agent until an answer comes back.
 *
 * <p>Mirrors Claude Code's {@code AskUserQuestion}, Copilot's clarification prompts, and Codex's
 * interactive ask. The reactor surfaces a question + a small set of choices to the chat UI, then
 * returns a payload the harness can render as a picker. Choices are 1–4 mutually exclusive
 * options unless {@code multi_select=true}; a free-text "Other" is always implicitly available
 * to the UI layer.
 *
 * <p>This reactor only renders/records the question. The actual round-trip (collecting the user's
 * answer back into the agent's context) is wired up by the harness that exposes the tool — see
 * the goal note that "the actual mechanism for exposing them to the model comes later".
 *
 * <p>Argument shape:
 * <ul>
 *   <li>{@code question} — required, the prompt shown to the user</li>
 *   <li>{@code options} — required, list of either strings or
 *       {@code {label, description?}} maps; 2–4 entries</li>
 *   <li>{@code header} — optional short chip label (≤12 chars)</li>
 *   <li>{@code multi_select} — optional boolean, default false</li>
 * </ul>
 */
public class AskUserQuestionReactor extends AbstractAgentToolReactor {

    private static final Logger logger = LogManager.getLogger(AskUserQuestionReactor.class);

    private static final int MIN_OPTIONS = 2;
    private static final int MAX_OPTIONS = 4;
    private static final int HEADER_MAX_CHARS = 12;

    public AskUserQuestionReactor() {
        this.keysToGet   = new String[] { "question", "options", "header", "multi_select" };
        this.keyRequired = new int[]    { 1,          1,         0,        0              };
    }

    @Override
    protected NounMetadata doExecute() throws Exception {
        String question = this.keyValue.get("question");
        if (question == null || question.trim().isEmpty()) {
            return new NounMetadata("Error: question is required", PixelDataType.CONST_STRING);
        }
        question = question.trim();

        @SuppressWarnings("rawtypes")
        List rawOptions = getList("options");
        if (rawOptions == null) {
            return new NounMetadata(
                    "Error: options must be a list of 2-4 strings or {label, description} objects",
                    PixelDataType.CONST_STRING);
        }
        if (rawOptions.size() < MIN_OPTIONS || rawOptions.size() > MAX_OPTIONS) {
            return new NounMetadata(
                    "Error: options must contain between " + MIN_OPTIONS + " and " + MAX_OPTIONS
                            + " entries (got " + rawOptions.size() + ")",
                    PixelDataType.CONST_STRING);
        }

        List<Map<String, String>> normalized = new ArrayList<>();
        for (int i = 0; i < rawOptions.size(); i++) {
            Object item = rawOptions.get(i);
            Map<String, String> opt = new LinkedHashMap<>();
            if (item instanceof String) {
                String label = ((String) item).trim();
                if (label.isEmpty()) {
                    return new NounMetadata(
                            "Error: options[" + i + "] is empty",
                            PixelDataType.CONST_STRING);
                }
                opt.put("label", label);
            } else if (item instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> m = (Map<String, Object>) item;
                Object label = m.get("label");
                if (label == null || String.valueOf(label).trim().isEmpty()) {
                    return new NounMetadata(
                            "Error: options[" + i + "].label is required",
                            PixelDataType.CONST_STRING);
                }
                opt.put("label", String.valueOf(label).trim());
                Object description = m.get("description");
                if (description != null && !String.valueOf(description).trim().isEmpty()) {
                    opt.put("description", String.valueOf(description).trim());
                }
            } else {
                return new NounMetadata(
                        "Error: options[" + i + "] must be a string or {label, description} object",
                        PixelDataType.CONST_STRING);
            }
            normalized.add(opt);
        }

        String header = this.keyValue.get("header");
        if (header != null) {
            header = header.trim();
            if (header.length() > HEADER_MAX_CHARS) {
                return new NounMetadata(
                        "Error: header must be <= " + HEADER_MAX_CHARS + " characters",
                        PixelDataType.CONST_STRING);
            }
        }

        boolean multiSelect = "true".equalsIgnoreCase(this.keyValue.get("multi_select"));

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "ask_user_question");
        payload.put("question", question);
        if (header != null && !header.isEmpty()) {
            payload.put("header", header);
        }
        payload.put("multi_select", multiSelect);
        payload.put("options", normalized);

        logger.info("[AgentTool] AskUserQuestion | insightId={} | question={} | options={}",
                insight.getInsightId(), question, normalized.size());

        return new NounMetadata(payload, PixelDataType.MAP);
    }

    @Override
    protected String getDescriptionForKey(String key) {
        switch (key) {
            case "question":     return "The question to display to the user.";
            case "options":      return "List of 2–4 choices. Each item is either a plain string or a {label, description?} object.";
            case "header":       return "Short chip label shown above the question (max 12 characters).";
            case "multi_select": return "If true, the user may select multiple options. Defaults to false.";
            default:             return super.getDescriptionForKey(key);
        }
    }

    @Override
    public String getReactorDescription() {
        return "Asks the user a structured question with 2-4 multiple-choice options and pauses "
             + "until they respond. Use header for a short chip label, multi_select=true to allow "
             + "multiple selections. Always include an explicit alternative if the answers might "
             + "not be exhaustive — the UI provides 'Other' for free-text.";
    }

    @Override
    protected MCP_KEY_TYPE getKeyTypeForMCP(String key) {
        if ("options".equals(key))      return MCP_KEY_TYPE.ARRAY;
        if ("multi_select".equals(key)) return MCP_KEY_TYPE.BOOLEAN;
        return super.getKeyTypeForMCP(key);
    }
}
