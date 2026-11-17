package prerna.collaboration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class BrainTopicVotesUnitTests {

	private static final String ROW = "\"name\": \"ForgeKit runner pools\", \"about\": \"Pool sizing and cost.\"}";

	@Test
	void anAnswerInTheAskedShapeIsRead() {
		Map<String, BrainTopicVotes.Vote> votes = BrainTopicVotes
				.validate("{\"clusters\": [{\"id\": \"k1\", \"kind\": \"work\", " + ROW + "]}", List.of("k1"));
		assertEquals("ForgeKit runner pools", votes.get("k1").name());
	}

	@Test
	void aModelNotHeldToTheSchemaMaySendTheListAloneAndNameTheKindItsOwnWay() {
		Map<String, BrainTopicVotes.Vote> votes = BrainTopicVotes
				.validate("[{\"id\": \"k1\", \"classification\": \"work\", " + ROW + "]", List.of("k1"));
		assertEquals("work", votes.get("k1").kind());
	}

	@Test
	void anAnswerThatLeavesAClusterOutOrMakesOneUpIsRefused() {
		assertNull(BrainTopicVotes.validate("[{\"id\": \"k1\", \"kind\": \"work\", " + ROW + "]", List.of("k1", "k2")));
		assertNull(BrainTopicVotes.validate("[{\"id\": \"k9\", \"kind\": \"work\", " + ROW + "]", List.of("k1")));
	}
}
