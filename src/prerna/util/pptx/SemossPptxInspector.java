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
package prerna.util.pptx;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.json.JSONArray;
import org.json.JSONObject;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityModelMetadataUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.om.Insight;
import prerna.reactor.agent.exceptions.AgentCancelledException;
import prerna.util.Utility;
import prerna.util.unoserver.Unoserver;

/** Connects the inspection service to SEMOSS permissions, media messages and model logging. */
public final class SemossPptxInspector {
    public static final String DEFAULT_MODEL_PROPERTY = "PPTX_VISION_MODEL_ID";
    public static final String RENDER_VERSION_PROPERTY = "PPTX_RENDER_CACHE_VERSION";

    private SemossPptxInspector() {}

    public static JSONObject inputSchema() {
        JSONObject properties = new JSONObject()
                .put("filePath", new JSONObject().put("type", "string").put("description", "PPTX path relative to the current working directory."))
                .put("instructions", new JSONObject().put("type", "string").put("description", "Required review brief: what to inspect, desired design, and constraints to preserve."))
                .put("slides", new JSONObject().put("type", "array").put("items", new JSONObject().put("type", "integer").put("minimum", 1))
                        .put("minItems", 1).put("maxItems", PptxRenderService.MAX_SELECTED_SLIDES).put("uniqueItems", true)
                        .put("description", "Original 1-based slide numbers. Omit to review the whole deck, including hidden slides; at most 100 per call."))
                .put("context", new JSONObject().put("type", "string").put("description", "Optional audience, branding, template requirements, or relevant content context."))
                .put("engine", new JSONObject().put("type", "string").put("description", "Optional vision model engine ID; defaults to PPTX_VISION_MODEL_ID, then this agent's model."))
                .put("checkConsistency", new JSONObject().put("type", "boolean").put("default", true)
                        .put("description", "Also compare the selected slides together. Default true; ignored for one slide."));
        return new JSONObject().put("type", "object").put("properties", properties)
                .put("required", new JSONArray(List.of("filePath", "instructions"))).put("additionalProperties", false);
    }

    public static JSONObject inspect(Path root, Map<String, Object> parameters, Insight insight,
            String parentRoomId, String fallbackModelId) throws Exception {
        if (insight == null || insight.getUser() == null) throw new IllegalArgumentException("An authenticated insight is required");
        String filePath = string(parameters, "filePath", true);
        String instructions = string(parameters, "instructions", true);
        String context = string(parameters, "context", false);
        String engineId = preflight(string(parameters, "engine", false), fallbackModelId, insight);
        IModelEngine model = Utility.getModel(engineId);
        List<Integer> selection = null;
        if (parameters.containsKey("slides")) {
            Object raw = parameters.get("slides");
            if (!(raw instanceof List<?>) && !(raw instanceof JSONArray)) throw new IllegalArgumentException("slides must be an array of integers");
            selection = PptxInspectionService.integers(raw instanceof JSONArray array ? array : new JSONArray((List<?>) raw));
        }
        Object consistency = parameters.getOrDefault("checkConsistency", Boolean.TRUE);
        if (!(consistency instanceof Boolean)) throw new IllegalArgumentException("checkConsistency must be a boolean");
        final long started = System.nanoTime();
        Runnable checkActive = () -> {
            if (Thread.currentThread().isInterrupted()) throw new AgentCancelledException("PPTX inspection cancelled");
            if (System.nanoTime() - started > java.time.Duration.ofMinutes(10).toNanos())
                throw new IllegalStateException("PPTX inspection exceeded its 10-minute time budget");
        };
        Unoserver uno = new Unoserver();
        PptxRenderService renderer = new PptxRenderService(new PptxRenderService.Converter() {
            @Override public byte[] convert(Path source) { return uno.convert(source.toFile(), "pdf"); }
            @Override public String cacheKey() { return uno.getBaseUrl() + "\n" + property(RENDER_VERSION_PROPERTY); }
        });
        List<String> modelRooms = new ArrayList<>();
        PptxInspectionService.VisionClient vision = (system, task, images, schema) -> {
            checkActive.run();
            Room room = RoomUtils.createRoomForStatelessAsk(UUID.randomUUID().toString(), insight, model,
                    "PPTX visual inspection", null, null, null, null, parentRoomId);
            modelRooms.add(room.getId());
            List<String> media = new ArrayList<>();
            Path roomPath = Path.of(room.getRoomFolderPath());
            Files.createDirectories(roomPath);
            for (Path image : images) {
                String name = "pptx-" + UUID.randomUUID() + ".png";
                Files.copy(image, roomPath.resolve(name));
                media.add(name);
            }
            Map<String, Object> params = new HashMap<>();
            params.put("use_history", false);
            params.put("stream", false);
            params.put("max_tokens", 2000);
            // The SEMOSS OpenAI message builder maps schema to strict response_format JSON Schema.
            // Unsupported deployments fail explicitly; never silently fall back to unconstrained prose.
            params.put("schema", schema.toMap());
            InputMessage input = InputMessage.builder(room).withSystemPrompt(system).withText(task)
                    .withModelType(model.getModelType()).withParamMap(params).withMediaInputs(media, room).build();
            ResponseMessage response = room.ask(input, model);
            checkActive.run();
            var result = response.getModelEngineResponse();
            if (result instanceof prerna.engine.impl.model.responses.AskErrorModelEngineResponse error)
                throw new IllegalStateException("Vision provider HTTP " + error.getCode() + ": " + error.getStringResponse());
            return new PptxInspectionService.VisionReply(response.getContent(),
                    result.getNumberOfTokensInPrompt() == null ? 0 : result.getNumberOfTokensInPrompt(),
                    result.getNumberOfTokensInResponse() == null ? 0 : result.getNumberOfTokensInResponse());
        };
        JSONObject report = new PptxInspectionService(renderer, vision).inspect(root, filePath, selection,
                instructions, context, (Boolean) consistency, checkActive);
        report.put("engine", engineId).put("modelRooms", new JSONArray(modelRooms));
        JSONObject artifacts = report.getJSONObject("artifacts");
        if (artifacts.has("report")) Files.writeString(root.resolve(artifacts.getString("report")), report.toString(2));
        return report;
    }

    /** Uses the same selection and permission checks as inspection, without model calls or rendering. */
    public static String preflight(String explicitEngine, String fallbackModelId, Insight insight) {
        if (insight == null || insight.getUser() == null) throw new IllegalArgumentException("An authenticated insight is required");
        String engineId = explicitEngine == null || explicitEngine.isBlank() ? property(DEFAULT_MODEL_PROPERTY) : explicitEngine.trim();
        if (engineId == null) engineId = fallbackModelId;
        if (engineId == null || engineId.isBlank()) throw new IllegalArgumentException("Set engine or configure " + DEFAULT_MODEL_PROPERTY);
        if (!SecurityEngineUtils.userCanViewEngine(insight.getUser(), engineId)
                || SecurityEngineUtils.getEngineType(engineId) != IEngine.CATALOG_TYPE.MODEL)
            throw new IllegalArgumentException("Vision model does not exist or is not accessible");
        validateVisionMetadata(SecurityModelMetadataUtils.getModelMetadata(engineId));
        return engineId;
    }

    static void validateVisionMetadata(Map<String, Object> metadata) {
        if (metadata == null) return; // Older engines may not have catalog metadata; let the provider validate.
        Object capability = metadata.get("capability");
        if (capability != null && !capability.toString().isBlank() && !"TEXT_GENERATION".equalsIgnoreCase(capability.toString()))
            throw new IllegalArgumentException("PPTX inspection requires a text-generation model with image input");
        Object modalities = metadata.get("inputModalities");
        if (modalities == null) return;
        JSONArray list = modalities instanceof List<?> values ? new JSONArray(values)
                : modalities instanceof JSONArray array ? array
                : modalities.toString().isBlank() ? new JSONArray() : new JSONArray(modalities.toString());
        if (!list.isEmpty()) {
            boolean image = false;
            for (Object item : list) image |= "IMAGE".equalsIgnoreCase(String.valueOf(item));
            if (!image) throw new IllegalArgumentException("Selected model does not support image input");
        }
    }

    private static String string(Map<String, Object> values, String key, boolean required) {
        Object raw = values.get(key);
        if (raw == null || raw instanceof String && ((String) raw).isBlank()) {
            if (required) throw new IllegalArgumentException(key + " is required");
            return null;
        }
        if (!(raw instanceof String)) throw new IllegalArgumentException(key + " must be a string");
        return ((String) raw).trim();
    }

    private static String property(String key) {
        String value = Utility.getDIHelperProperty(key);
        if (value == null || value.isBlank()) value = System.getenv(key);
        return value == null || value.isBlank() ? null : value.trim();
    }
}
