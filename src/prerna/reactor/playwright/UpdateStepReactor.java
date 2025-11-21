package prerna.reactor.playwright;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class UpdateStepReactor extends AbstractReactor {

	private record UpdateResult(ScreenshotResponse screenshot, List<PlaywrightStep> updatedSteps) {
	}

	public UpdateStepReactor() {
		this.keysToGet = new String[] { "sessionId", "tabId", "inputs" };
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
		List<PlaywrightStep> stepList = mapper.convertValue(steps, new TypeReference<List<PlaywrightStep>>() {
		});

		UpdateResult result = updateStep(sessionId, tabId, stepList);

		HashMap<String, Object> response = new HashMap<>();
		response.put("screenshot", result.screenshot);
		response.put("updatedSteps", result.updatedSteps);

		return new NounMetadata(response, PixelDataType.MAP);
	}

	private UpdateResult updateStep(String sessionId, String tabId, List<PlaywrightStep> inputs) {
		PlaywrightSession session = this.insight.getUser().getPlaywrightSession(sessionId);
		List<PlaywrightStep> updatedSteps = new ArrayList<>();

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
                        Step existingStep = list.get(index);
                        Step updatedStep = updateStep(existingStep, step);
                        list.set(index, updatedStep); // update the step in place
                        updatedSteps.add(updatedStep);
                        
                        
                        // no need for re-execution of type step -- uncomment me if you want to execute the edited step

//                        if (updatedStep.type() == StepType.TYPE && !Objects.equals(existingStep.text(), updatedStep.text())) {
//                            SessionUtility.applyStep(session, updatedStep, tabId);
//                        }
                    }, () -> {
                        throw new IllegalArgumentException("Step with ID " + step.id() + " not found.");
                    });
        }

		ScreenshotResponse screenshot = ScreenshotReactor.screenshot(session, tabId);
		return new UpdateResult(screenshot, updatedSteps);
	}

	private PlaywrightStep updateStep(PlaywrightStep existing, PlaywrightStep input) {
		String label = input.label() != null ? input.label() : existing.label();
		String text = input.text() != null ? input.text() : existing.text();
		String description = input.description() != null ? input.description() : existing.description();
		Boolean shouldRun = input.shouldRun() != null ? input.shouldRun() : existing.shouldRun();
		Boolean required = input.required() != null ? input.required() : existing.required();
		boolean storeValue = input.storeValue(); // primitive boolean, always has a value

		if (existing.isPassword()) {
			return new PlaywrightStep(existing, label, "", false, description, shouldRun != null ? shouldRun : false,
					required != null ? required : false);
		} else {
			return new PlaywrightStep(existing, label, text, storeValue, description,
					shouldRun != null ? shouldRun : false, required != null ? required : false);
		}
	}

	@Override
	public String getReactorDescription() {
		return "Reactor that Update a step ";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("sessionId")) {
			return "The session ID of the current playwright session";
		} else if (key.equals("tabID")) {
			return "The tab ID of the current playwright session";
		} else if (key.equals("inputs")) {
			return "the inputs that need to be updated";
		}

		return super.getDescriptionForKey(key);
	}
}
