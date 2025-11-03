package prerna.reactor.playwright;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.stream.IntStream;

public class UpdateStepReactor extends AbstractReactor {

    public UpdateStepReactor(){
        this.keysToGet = new String[] {
                "sessionId",
                "tabId",
                "inputs"
        };
        this.keyRequired = new int[] { 1, 1, 1 };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String sessionId = this.keyValue.get(this.keysToGet[0]);
        String tabId = this.keyValue.get(this.keysToGet[1]);

        GenRowStruct inputs = this.store.getGenRowStruct("inputs");
        List<Object> steps = inputs.getAllValues();
        ObjectMapper mapper = new ObjectMapper();
        List<Step> stepList = mapper.convertValue(steps, new TypeReference<List<Step>>() {});

        HashMap<String, Object> response = new HashMap<>();
        response.put("screenshot", updateStep(sessionId, tabId, stepList));

        return new NounMetadata(response, PixelDataType.MAP);
    }

    private ScreenshotResponse updateStep(String sessionId, String tabId, List<Step> inputs) {
        Session session = this.insight.getUser().getPlaywrightSession(sessionId);

        for (Step step : inputs) {
            session.history.steps().get(tabId).stream()                       // Stream<List<Step>>
                    .flatMap(outer -> IntStream.range(0, outer.size())
                            .mapToObj(i -> new Object[]{outer, i}))        // carry list + index
                    .filter(a -> ((List<Step>) a[0]).get((int) a[1]).id() == step.id())
                    .findFirst()
                    .ifPresentOrElse(a -> {
                        @SuppressWarnings("unchecked")
                        List<Step> list = (List<Step>) a[0];
                        int index = (int) a[1];
                        Step updatedStep = updateStep(list.get(index), step);
                        SessionUtility.applyStep(session, updatedStep, tabId); // re-apply the updated step
                        list.set(index, updatedStep); // update the step in place
                    }, () -> {
                        throw new IllegalArgumentException("Step with ID " + step.id() + " not found.");
                    });
        }
        return ScreenshotReactor.screenshot(session, tabId);
    }

    private Step updateStep(Step existing, Step input) {

        return new Step(existing, input.label(), input.text(), input.storeValue());
    }
}
