package prerna.reactor.playwright;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetAllStepsReactor extends AbstractReactor {

    public GetAllStepsReactor() {
        this.keysToGet = new String[] {
                "sessionId",
                "fileName"
        };
        this.keyRequired = new int[] { 1, 1 };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();

        String sessionId = this.keyValue.get(this.keysToGet[0]);
        String fileName = this.keyValue.get(this.keysToGet[1]);

        if (sessionId == null || sessionId.isEmpty()) {
            throw new IllegalArgumentException("sessionId is required");
        }

        if (fileName == null || fileName.isEmpty()) {
            throw new IllegalArgumentException("fileName is required");
        }

        Map<String, Object> result = getAllSteps(sessionId, fileName);
        return new NounMetadata(result, PixelDataType.MAP);
    }

    private Map<String, Object> getAllSteps(String sessionId, String fileName) {
        Map<String, Object> response = new HashMap<>();

        // Load steps from file
        StepsEnvelope env = PlaywrightUtility.loadStepsFromFile(fileName);

        if (env == null) {
            throw new IllegalStateException("Failed to load steps from file: " + fileName);
        }

        // Transform the steps from Map<String, List<List<Step>>> to the frontend format
        Map<String, List<Map<String, Object>>> transformedSteps = new HashMap<>();

        for (Map.Entry<String, List<List<Step>>> entry : env.steps().entrySet()) {
            String tabId = entry.getKey();
            List<List<Step>> pages = entry.getValue();

            List<Map<String, Object>> tabSteps = new ArrayList<>();
            for (List<Step> page : pages) {
                for (Step step : page) {
                    Map<String, Object> stepMap = new HashMap<>();
                    stepMap.put("id", step.id());
                    stepMap.put("type", step.type().toString());

                    // Add relevant fields based on type
                    if (step.url() != null) {
                        stepMap.put("url", step.url());
                    }
                    if (step.coords() != null) {
                        Map<String, Object> coordsMap = new HashMap<>();
                        coordsMap.put("x", step.coords().x());
                        coordsMap.put("y", step.coords().y());
                        stepMap.put("coords", coordsMap);
                    }

                    if (step.text() != null) {
                        stepMap.put("text", step.text());
                    }
                    if (step.label() != null) {
                        stepMap.put("label", step.label());
                    }
                    if (step.type() == StepType.TYPE) {
                        stepMap.put("isPassword", step.isPassword());
                        stepMap.put("storeValue", step.storeValue());

                    }
                    if (step.deltaY() != null) {
                        stepMap.put("deltaY", step.deltaY());
                    }
                    if (step.type() == StepType.CONTEXT) {
                        if (step.multiCoords() != null) {
                            List<Map<String, Object>> multiCoordsList = new ArrayList<>();
                            for (Coords coord : step.multiCoords()) {
                                Map<String, Object> coordMap = new HashMap<>();
                                coordMap.put("x", coord.x());
                                coordMap.put("y", coord.y());
                                multiCoordsList.add(coordMap);
                            }
                            stepMap.put("multiCoords", multiCoordsList);
                        }
                        if (step.prompt() != null) {
                            stepMap.put("prompt", step.prompt());
                        }
                    }
                    if (step.waitAfterMs() != null) {
                        stepMap.put("waitAfterMs", step.waitAfterMs());
                    }

                    tabSteps.add(stepMap);
                }
            }

            transformedSteps.put(tabId, tabSteps);
        }

        response.put("steps", transformedSteps);
        response.put("success", true);
        response.put("sessionId", sessionId);

        return response;
    }
}

