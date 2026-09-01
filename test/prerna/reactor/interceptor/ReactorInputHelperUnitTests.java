package prerna.reactor.interceptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

class ReactorInputHelperUnitTests {

	@Test
	void resolvesValuesInsideMapArguments() {
		ReactorInputHelper helper = helper(Map.of("arg0",
				Map.of("subject", "Quarterly report", "message", "The report is attached.")));

		assertEquals("Quarterly report", helper.getMethodArgument("arg0.subject"));
		assertEquals("The report is attached.", helper.getMethodArgument("arg0.message"));
	}

	@Test
	void indexesAndProjectsAcrossListResults() {
		Map<String, Object> result = Map.of("messages",
				List.of(Map.of("subject", "First", "body", "Body one"),
						Map.of("subject", "Second", "body", "Body two")));
		ReactorInputHelper helper = helper(Map.of("result", result));

		assertEquals("Second", helper.getMethodArgument("result.messages.1.subject"));
		assertEquals(List.of("Body one", "Body two"), helper.getMethodArgument("result.messages.*.body"));
	}

	@Test
	@SuppressWarnings("unchecked")
	void nestedReplacementCopiesTheCallersMap() {
		Map<String, Object> originalMail = Map.of("subject", "Status", "message", "Account 123");
		Map<String, Object> arguments = new HashMap<>();
		arguments.put("arg0", originalMail);
		ReactorInputHelper helper = helper(arguments);

		assertTrue(helper.setMethodArgument("arg0.message", "Account [masked]"));

		Map<String, Object> guardedMail = (Map<String, Object>) arguments.get("arg0");
		assertNotSame(originalMail, guardedMail);
		assertEquals("Account 123", originalMail.get("message"));
		assertEquals("Account [masked]", guardedMail.get("message"));
		assertFalse(helper.setMethodArgument("arg0.missing", "value"));
	}

	private ReactorInputHelper helper(Map<String, Object> arguments) {
		NounStore nounStore = new NounStore("test");
		GenRowStruct argumentNoun = new GenRowStruct();
		argumentNoun.add(new NounMetadata(arguments, PixelDataType.MAP));
		nounStore.addNoun(PipelineReactorUtils.ARGUMENTS, argumentNoun);
		return new ReactorInputHelper(nounStore);
	}
}
