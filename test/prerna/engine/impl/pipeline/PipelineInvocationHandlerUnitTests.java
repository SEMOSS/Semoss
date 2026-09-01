package prerna.engine.impl.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.reactor.interceptor.PipelineReactorUtils;

class PipelineInvocationHandlerUnitTests {

	@Test
	void inputAuditPayloadIsCapturedBeforeLaterMutation() throws Exception {
		Method method = AuditTarget.class.getDeclaredMethod("execute", String.class, Map.class);
		Map<String, Object> parameters = new LinkedHashMap<>();
		parameters.put("threshold", 0.8);

		String snapshot = PipelineInvocationHandler.serializeAuditPayload(method,
				new Object[] { "masked input", parameters }, null, false);
		parameters.put("interim_result", Map.of("pass", true));
		parameters.put("result", "late response");

		JsonObject payload = JsonParser.parseString(snapshot).getAsJsonObject();
		assertEquals(2, payload.size());
		assertFalse(payload.has(PipelineReactorUtils.RESULT));
		assertFalse(snapshot.contains("late response"));
		assertFalse(snapshot.contains(PipelineReactorUtils.INTERIM_RESULT));
	}

	@Test
	void outputAuditPayloadContainsOnlyArgumentsAndForwardedResult() throws Exception {
		Method method = AuditTarget.class.getDeclaredMethod("execute", String.class, Map.class);

		String snapshot = PipelineInvocationHandler.serializeAuditPayload(method,
				new Object[] { "masked input", Map.of("threshold", 0.8) }, "guarded output", true);

		JsonObject payload = JsonParser.parseString(snapshot).getAsJsonObject();
		assertEquals(3, payload.size());
		assertEquals("guarded output", payload.get(PipelineReactorUtils.RESULT).getAsString());
		assertFalse(payload.has(PipelineReactorUtils.CONFIG));
		assertFalse(payload.has(PipelineReactorUtils.INTERIM_RESULT));
		assertTrue(snapshot.contains("masked input"));
	}

	private static final class AuditTarget {

		@SuppressWarnings("unused")
		private String execute(String input, Map<String, Object> parameters) {
			return input;
		}
	}
}
