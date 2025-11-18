package prerna.reactor.playwright;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;


public class SkipStepReactor extends AbstractReactor {

    ObjectMapper json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    Map<String, Object> response = new HashMap<>();
    private final static String REACTOR_DESCRIPTION = "Skip the current step in the playwright session.";
    private final static String SESSION_ID_KEY_DESCRIPTION = "Playwright session ID that stores information about the history of actions done during that session.";
    private final static String FILE_NAME_KEY_DESCRIPTION = "File name containing the steps to be replayed.";
    static StepsEnvelope stepsEnvelope;
    public static Path recordingsDir = PlaywrightUtility.initRecordingsDir();

    public SkipStepReactor() {
        this.keysToGet = new String[]{
                "sessionId",
                "fileName",
                "tabId"
        };
        this.keyRequired = new int[]{1, 1};
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String sessionId = this.keyValue.get(this.keysToGet[0]);
        String fileName = this.keyValue.get(this.keysToGet[1]);
        String tabId = this.keyValue.get(this.keysToGet[2]);

        if (sessionId == null || sessionId.isEmpty()) {
            throw new IllegalArgumentException("sessionId is required");
        }
        if (fileName == null || fileName.isEmpty()) {
            throw new IllegalArgumentException("fileName is required");
        }
        Session session = this.insight.getUser().getPlaywrightSession(sessionId);

        if (session == null) {
            throw new IllegalStateException("Session not found: " + sessionId);
        }

        // Load steps from file
        stepsEnvelope = loadStepsFromFile(fileName);
        List<List<Step>> allStepsList = stepsEnvelope.steps().entrySet().iterator().next().getValue();

        // Skip the current step
        skipStep(session, allStepsList, tabId);

        // Return updated session state
        return new NounMetadata(response, PixelDataType.MAP);
    }

    private void skipStep(Session session, List<List<Step>> allStepsList, String tabId) {
        // Validate inputs
        if (allStepsList == null || allStepsList.isEmpty()) {
            session.isLastPage = true;
            response.put("isLastPage", true);
            return;
        }

        // Validate current page index
        if (session.getCurrentPageIndex(tabId) < 0 || session.getCurrentPageIndex(tabId) >= allStepsList.size()) {
            session.isLastPage = true;
            response.put("isLastPage", true);
            return;
        }

        session.incrementStepIndex(tabId);

        // Check if the current step index exceeds the steps in the current page
        List<Step> currentPageSteps = allStepsList.get(session.getCurrentPageIndex(tabId));
        if (currentPageSteps != null && session.getCurrentStepIndex(tabId) >= currentPageSteps.size()) {
            // Move to the next page if there are more pages
            if (session.getCurrentPageIndex(tabId) < allStepsList.size() - 1) {
                session.incrementPageIndex(tabId);
                session.setCurrentStepIndex(tabId, 0);// Reset step index for the new page
            } else {
                // If no more pages, set the session to the last page
                session.isLastPage = true;
            }
        }
        response.put("isLastPage", session.isLastPage);
        if (session.getCurrentPageIndex(tabId) < allStepsList.size()) {
            response.put("actions", getPageActions(allStepsList.get(session.getCurrentPageIndex(tabId)), session.getCurrentStepIndex(tabId)));
        }
    }

    public static List<Map<String, Object>> getPageActions(List<Step> steps, int currentStepIndex) {
        List<Map<String, Object>> actionsList = new ArrayList<>();
        if (steps == null || steps.isEmpty()) {
            return actionsList;
        }
        if (currentStepIndex < 0 || currentStepIndex >= steps.size()) {
            return actionsList; // or throw exception
        }
        for (int i = currentStepIndex; i < steps.size(); i++) {
            Map<String, Object> action = new HashMap<>();
            Step current = steps.get(i);
            switch (current.type()) {
                case TYPE:
                    Map<String, Object> typeAction = new HashMap<>();
                    typeAction.put("label", current.label());
                    typeAction.put("text", current.text());
                    typeAction.put("isPassword", current.isPassword());
                    typeAction.put("coords", current.coords());
                    typeAction.put("storeValue", current.storeValue());
                    typeAction.put("selector", current.selector());
                    action.put("TYPE", typeAction);
                    break;
                case CLICK:
                    action.put("CLICK", current.coords());
                    break;
                case SCROLL:
                    action.put("SCROLL", current.deltaY());
                    break;
                case NAVIGATE:
                    action.put("NAVIGATE", current.url());
                    break;
                case WAIT:
                    action.put("WAIT", current.waitAfterMs());
                    break;
                case CONTEXT:
                    action.put("CONTEXT", Map.of(
                            current.multiCoords(), current.prompt()
                    ));
                    break;
                default:
                    break;
            }
            actionsList.add(action);
        }
        return actionsList;
    }

    public StepsEnvelope loadStepsFromFile(String nameOrPath) {

        Path file = nameOrPath.contains(FileSystems.getDefault().getSeparator())
                ? Paths.get(nameOrPath)
                : recordingsDir.resolve(nameOrPath.endsWith(".json") ? nameOrPath : nameOrPath + ".json");

        try {
            return json.readValue(file.toFile(), StepsEnvelope.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to read: " + file, e);
        }
    }

    @Override
    public String getReactorDescription() {
        return REACTOR_DESCRIPTION;
    }

    @Override
    protected String getDescriptionForKey(String key) {
        switch (key) {
            case "sessionId":
                return SESSION_ID_KEY_DESCRIPTION;
            case "fileName":
                return FILE_NAME_KEY_DESCRIPTION;
            default:
                return null;
        }
    }

}
