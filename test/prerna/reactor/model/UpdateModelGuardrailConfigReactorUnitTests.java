package prerna.reactor.model;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.reactor.interceptor.GenericGuardrailInputReactor;

class UpdateModelGuardrailConfigReactorUnitTests {

	@Test
	void acceptsOneFailureAction() {
		Map<String, Object> blockParams = validParams();
		blockParams.put("closeRoomOnBlock", true);
		blockParams.put("blockErrorMessage", "The request was blocked by a guardrail.");
		assertDoesNotThrow(() -> UpdateModelGuardrailConfigReactor.validateConfigStructure(config(blockParams)));

		Map<String, Object> responseParams = validParams();
		responseParams.put("respondWithGuardrailMessage", true);
		assertDoesNotThrow(() -> UpdateModelGuardrailConfigReactor.validateConfigStructure(config(responseParams)));
	}

	@Test
	void validatesRuntimeBlockResponseOptionTypes() {
		assertInvalidParam("respondWithGuardrailMessage", "true", "must be a boolean");
		assertInvalidParam("closeRoomOnBlock", "true", "must be a boolean");
		assertInvalidParam("blockErrorMessage", true, "must be a non-empty string");
		assertInvalidParam("blockErrorMessage", " ", "must be a non-empty string");
	}

	@Test
	void rejectsConflictingFailureActions() {
		Map<String, Object> params = validParams();
		params.put("blockOnGuardrailFailure", true);
		params.put("respondWithGuardrailMessage", true);

		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
				() -> UpdateModelGuardrailConfigReactor.validateConfigStructure(config(params)));
		assertTrue(exception.getMessage().contains("must enable exactly one failure action"));
	}

	@Test
	void rejectsBlockOnlyOptionsForAnotherFailureAction() {
		Map<String, Object> params = validParams();
		params.put("respondWithGuardrailMessage", true);
		params.put("closeRoomOnBlock", true);

		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
				() -> UpdateModelGuardrailConfigReactor.validateConfigStructure(config(params)));
		assertTrue(exception.getMessage().contains("can only be enabled when blocking on failure"));
	}

	@Test
	void maskingAcceptsAnyMappedParameterName() {
		Map<String, Object> params = validParams();
		params.put("blockOnGuardrailFailure", false);
		params.put("maskOnGuardrailFailure", true);
		assertDoesNotThrow(() -> UpdateModelGuardrailConfigReactor.validateConfigStructure(config(params)));

		// the masked value is written back to the argument that supplied it, so the
		// guardrail's own parameter name carries no meaning here
		Map<String, Object> customName = validParams();
		customName.put("blockOnGuardrailFailure", false);
		customName.put("maskOnGuardrailFailure", true);
		customName.put("inputMapping", Map.of("content", "arg1"));
		assertDoesNotThrow(() -> UpdateModelGuardrailConfigReactor.validateConfigStructure(config(customName)));
	}

	@Test
	void maskingRejectsWhenNoMappedArgumentCanReceiveTheValue() {
		Map<String, Object> combinedMapping = validParams();
		combinedMapping.put("blockOnGuardrailFailure", false);
		combinedMapping.put("maskOnGuardrailFailure", true);
		combinedMapping.put("inputMapping", Map.of("content", List.of("arg0", "arg1")));
		IllegalArgumentException combined = assertThrows(IllegalArgumentException.class,
				() -> UpdateModelGuardrailConfigReactor.validateConfigStructure(config(combinedMapping)));
		assertTrue(combined.getMessage().contains("to a single argument name"));

		Map<String, Object> overriddenMapping = validParams();
		overriddenMapping.put("blockOnGuardrailFailure", false);
		overriddenMapping.put("maskOnGuardrailFailure", true);
		overriddenMapping.put("directParameters", Map.of("prompt", "fixed value"));
		IllegalArgumentException overridden = assertThrows(IllegalArgumentException.class,
				() -> UpdateModelGuardrailConfigReactor.validateConfigStructure(config(overriddenMapping)));
		assertTrue(overridden.getMessage().contains("does not override"));
	}

	private static void assertInvalidParam(String key, Object value, String expectedMessage) {
		Map<String, Object> params = validParams();
		params.put(key, value);

		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
				() -> UpdateModelGuardrailConfigReactor.validateConfigStructure(config(params)));
		assertTrue(exception.getMessage().contains(expectedMessage));
	}

	private static Map<String, Object> validParams() {
		Map<String, Object> params = new HashMap<>();
		params.put("guardrailEngineId", "guardrail-engine-id");
		params.put("inputMapping", Map.of("prompt", "arg0"));
		return params;
	}

	private static Map<String, Object> config(Map<String, Object> params) {
		Map<String, Object> guardrail = Map.of("reactorClass", GenericGuardrailInputReactor.class.getName(), "params",
				params);
		Map<String, Object> pipeline = Map.of("input", List.of(guardrail));
		return Map.of("pipelines", Map.of("askCall", pipeline));
	}
}
