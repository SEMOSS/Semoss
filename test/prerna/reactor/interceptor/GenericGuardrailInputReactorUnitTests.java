package prerna.reactor.interceptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.message.InputMessage;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

class GenericGuardrailInputReactorUnitTests {

	@Test
	void maskingUpdatesTheGuardedInputMessageInstance() {
		Room room = new Room();
		room.setId("room-id");
		InputMessage inputMessage = InputMessage.builder(room).withText("api_key=secret").build();

		Map<String, Object> arguments = new HashMap<>();
		arguments.put("arg0", inputMessage);

		ReactorInputHelper helper = helperFor(arguments);
		boolean replaced = GenericGuardrailInputReactor.replaceGuardedInput(helper, Map.of("prompt", "arg0"),
				"api_key=[masked]");

		assertTrue(replaced);
		assertSame(inputMessage, arguments.get("arg0"));
		assertEquals("api_key=[masked]", inputMessage.getFullInputPrompt());
		assertEquals("api_key=[masked]", inputMessage.getInputUIPrompt());
	}

	@Test
	void maskingWritesBackUnderAnyGuardrailParameterName() {
		Map<String, Object> arguments = new HashMap<>();
		arguments.put("arg0", "/vault/notes.txt");
		arguments.put("arg1", "api_key=secret");

		// only the mapped argument is a candidate, so arg0 holding text as well
		// does not make the target ambiguous
		ReactorInputHelper helper = helperFor(arguments);
		boolean replaced = GenericGuardrailInputReactor.replaceGuardedInput(helper, Map.of("content", "arg1"),
				"api_key=[masked]");

		assertTrue(replaced);
		assertEquals("api_key=[masked]", arguments.get("arg1"));
		assertEquals("/vault/notes.txt", arguments.get("arg0"));
	}

	@Test
	void maskingSkipsMappedArgumentsThatHoldNoText() {
		Map<String, Object> arguments = new HashMap<>();
		arguments.put("arg0", "sensitive body");
		arguments.put("arg1", Map.of("bucket", "reports"));

		ReactorInputHelper helper = helperFor(arguments);
		boolean replaced = GenericGuardrailInputReactor.replaceGuardedInput(helper,
				Map.of("content", "arg0", "options", "arg1"), "[masked]");

		assertTrue(replaced);
		assertEquals("[masked]", arguments.get("arg0"));
		assertEquals(Map.of("bucket", "reports"), arguments.get("arg1"));
	}

	@Test
	void maskingWritesBackInsideAMapArgument() {
		Map<String, Object> request = new HashMap<>();
		request.put("body", "api_key=secret");
		Map<String, Object> arguments = new HashMap<>();
		arguments.put("arg0", request);

		ReactorInputHelper helper = helperFor(arguments);
		boolean replaced = GenericGuardrailInputReactor.replaceGuardedInput(helper, Map.of("content", "arg0.body"),
				"api_key=[masked]");

		assertTrue(replaced);
		assertEquals("api_key=[masked]", ((Map<?, ?>) arguments.get("arg0")).get("body"));
	}

	@Test
	void maskingIsRefusedWhenTwoMappedArgumentsHoldText() {
		Map<String, Object> arguments = new HashMap<>();
		arguments.put("arg0", "the question");
		arguments.put("arg1", "the context");

		ReactorInputHelper helper = helperFor(arguments);
		boolean replaced = GenericGuardrailInputReactor.replaceGuardedInput(helper,
				Map.of("question", "arg0", "context", "arg1"), "[masked]");

		// one returned value cannot be attributed to either argument
		assertFalse(replaced);
		assertEquals("the question", arguments.get("arg0"));
		assertEquals("the context", arguments.get("arg1"));
	}

	@Test
	void maskingIsRefusedWhenTheMappingCombinesArguments() {
		Map<String, Object> arguments = new HashMap<>();
		arguments.put("arg0", "the question");
		arguments.put("arg1", "the context");

		ReactorInputHelper helper = helperFor(arguments);
		boolean replaced = GenericGuardrailInputReactor.replaceGuardedInput(helper,
				Map.of("content", List.of("arg0", "arg1")), "[masked]");

		assertFalse(replaced);
		assertEquals("the question", arguments.get("arg0"));
	}

	@Test
	void maskingIsRefusedWhenNoMappedArgumentHoldsText() {
		Map<String, Object> arguments = new HashMap<>();
		arguments.put("arg0", List.of("chunk one", "chunk two"));

		ReactorInputHelper helper = helperFor(arguments);
		boolean replaced = GenericGuardrailInputReactor.replaceGuardedInput(helper, Map.of("content", "arg0"),
				"[masked]");

		// a list cannot take a single masked string without changing its type
		assertFalse(replaced);
		assertEquals(List.of("chunk one", "chunk two"), arguments.get("arg0"));
	}

	/**
	 * Builds the helper the interceptor hands to a guardrail, over the given
	 * intercepted arguments.
	 *
	 * @param arguments intercepted method arguments, keyed by argument name
	 * @return a helper reading and writing that map
	 */
	private static ReactorInputHelper helperFor(Map<String, Object> arguments) {
		NounStore nounStore = new NounStore("input-pipeline");
		GenRowStruct argumentRow = new GenRowStruct();
		argumentRow.add(new NounMetadata(arguments, PixelDataType.MAP));
		nounStore.addNoun(PipelineReactorUtils.ARGUMENTS, argumentRow);
		return new ReactorInputHelper(nounStore);
	}
}
