package prerna.engine.impl.guardrail;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.message.InputMessage;

class PolicyComplianceGuardrailEngineUnitTests {

	@Test
	void combinesSelectedTextValuesForOneReview() {
		assertEquals("Subject\nFirst body\nSecond body",
				PromptGuardrailEngine.extractText(List.of("Subject", "First body", "Second body")));
	}

	@Test
	void combinesSubjectsAndBodiesFromMailboxResults() {
		Map<String, Object> result = Map.of("messages", List.of(Map.of("subject", "First", "body", "Body one"),
				Map.of("subject", "Second", "body", "Body two")));

		assertEquals("First\nBody one\nSecond\nBody two", PromptGuardrailEngine.extractText(result));
	}

	@Test
	void extractsAndMasksTheTextOnAnInputMessage() {
		Room room = new Room();
		room.setId("room-id");
		InputMessage original = InputMessage.builder(room).withSystemPrompt("Be concise").withText("api_key=secret")
				.withParamMap(Map.of("temperature", 0.2)).build();

		assertEquals("api_key=secret", PromptGuardrailEngine.extractText(original));
		assertEquals("api_key=secret", original.getInputText());

		String messageId = original.getMessageId();
		original.setFullInputPrompt("api_key=[masked]");
		assertEquals("api_key=[masked]", original.getFullInputPrompt());
		assertEquals("api_key=[masked]", original.getInputUIPrompt());
		assertEquals(messageId, original.getMessageId());
		assertEquals("Be concise", original.getSystemPrompt());
		assertEquals(Map.of("temperature", 0.2), original.getParamMap());
	}

	@Test
	void toolResultMessagesExposeAndMaskTheirModelBoundOutput() {
		Room room = new Room();
		room.setId("room-id");
		InputMessage original = InputMessage.toolExecution(room, "call-1", "lookup", "secret result",
				Map.of("query", "record"), "success", false);

		assertNull(original.getInputText());
		assertEquals("secret result", original.getFullInputPrompt());
		original.setFullInputPrompt("[masked result]");
		assertEquals("[masked result]", original.getFullInputPrompt());
	}

	@Test
	void llmGuardrailsShareThePromptGuardrailBase() {
		AggressiveSelfHarmGuardrailEngine selfHarm = new AggressiveSelfHarmGuardrailEngine();
		PolicyComplianceGuardrailEngine policy = new PolicyComplianceGuardrailEngine();

		assertTrue(selfHarm instanceof PromptGuardrailEngine);
		assertTrue(policy instanceof PromptGuardrailEngine);
		assertTrue(selfHarm.getDefaultSystemPrompt().contains("self"));
		assertTrue(policy.getDefaultSystemPrompt().contains("${POLICY_DESCRIPTION}"));
		assertFalse(selfHarm.isFailOpenByDefault());
		assertTrue(policy.isFailOpenByDefault());
	}

	@Test
	void defaultMarkdownContainsAReadyToCustomizePipeline() {
		AggressiveSelfHarmGuardrailEngine selfHarm = new AggressiveSelfHarmGuardrailEngine();
		selfHarm.setEngineId("self-harm-id");
		String selfHarmMarkdown = selfHarm.getDefaultMarkdown();

		assertTrue(selfHarmMarkdown.contains("\"guardrailEngineId\": \"self-harm-id\""));
		assertTrue(selfHarmMarkdown.contains("\"askRoom\""));

		PolicyComplianceGuardrailEngine policy = new PolicyComplianceGuardrailEngine();
		policy.setEngineId("policy-id");
		String policyMarkdown = policy.getDefaultMarkdown();

		assertTrue(policyMarkdown.contains("\"guardrailEngineId\": \"policy-id\""));
		assertTrue(policyMarkdown.contains("\"policy\""));

		Map<String, AbstractGuardrailReactorFunctionEngine> additionalGuardrails = Map.of("detoxify-id",
				new DetoxifyGuardrailEngine(), "gliner-id", new GLiNERGuardrailEngine(), "on-topic-id",
				new OnTopicGuardrailEngine(), "prompt-injection-id", new PromptInjectionGuardrailEngine(),
				"local-python-id", new LocalPythonGuardrailReactorFunctionEngine());
		for (Map.Entry<String, AbstractGuardrailReactorFunctionEngine> entry : additionalGuardrails.entrySet()) {
			entry.getValue().setEngineId(entry.getKey());
			String markdown = entry.getValue().getDefaultMarkdown();
			assertTrue(markdown.contains("\"guardrailEngineId\": \"" + entry.getKey() + "\""));
			assertTrue(markdown.contains("\"prompt\": \"arg0\""));
		}
	}
}
