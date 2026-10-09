package prerna.collaboration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import prerna.collaboration.BrainMemoryRecall.Line;
import prerna.collaboration.BrainMemoryRecall.Recall;
import prerna.collaboration.BrainMemoryRecall.Scope;
import prerna.collaboration.BrainMemoryUtils.Memory;
import prerna.collaboration.BrainMemoryUtils.Ref;
import prerna.collaboration.BrainMemoryUtils.Source;
import prerna.collaboration.BrainRulesGate.Rule;

class BrainMemoryRecallUnitTests {

	private static final Timestamp NOW = Timestamp.valueOf("2026-10-06 12:00:00");
	private static final Ref PRIYA = new Ref(BrainMemoryUtils.PERSON, "p-priya");
	private static final Ref DANA = new Ref(BrainMemoryUtils.PERSON, "p-dana");
	private static final Ref ACME = new Ref(BrainMemoryUtils.TOPIC, "t-acme");
	private static final Ref ACME_ACCOUNT = new Ref(BrainMemoryUtils.ACCOUNT, "a-acme");
	private static final Ref THIS_THREAD = new Ref(BrainMemoryUtils.THREAD, "th-1");
	private static final Ref OTHER_THREAD = new Ref(BrainMemoryUtils.THREAD, "th-2");

	private static final Scope SCOPE = new Scope("th-1", Set.of("p-priya"), Set.of("t-acme"), Set.of("a-acme"), false,
			Set.of("p-dana"));

	private static Memory memory(String id, String kind, String text, boolean confirmed, String updated, Ref... about) {
		Timestamp at = Timestamp.valueOf(updated);
		return new Memory(id, kind, text, BrainMemoryUtils.ACTIVE, confirmed ? "you" : BrainMemoryUtils.ASSISTANT,
				confirmed, false, null, null, Source.ui(), at, at, confirmed ? at : null, List.of(about));
	}

	private static Memory fact(String id, Ref... about) {
		return memory(id, BrainMemoryUtils.FACT, "Fact " + id, true, "2026-10-01 09:00:00", about);
	}

	@Test
	void bucketsSayWhyAMemoryApplies() {
		Memory pinned = new Memory("m0", BrainMemoryUtils.FACT, "Pinned", BrainMemoryUtils.ACTIVE, "you", true, true,
				null, null, Source.ui(), NOW, NOW, NOW, List.of(OTHER_THREAD));
		assertEquals(BrainMemoryRecall.PINNED, BrainMemoryRecall.bucket(pinned, SCOPE));
		assertEquals(BrainMemoryRecall.PREFERENCES, BrainMemoryRecall.bucket(
				memory("m1", BrainMemoryUtils.PREFERENCE, "Sign as Rob", true, "2026-10-01 09:00:00"), SCOPE));
		assertEquals(BrainMemoryRecall.GENERAL, BrainMemoryRecall.bucket(fact("m2"), SCOPE));
		assertEquals(BrainMemoryRecall.THIS_THREAD, BrainMemoryRecall.bucket(fact("m3", THIS_THREAD, PRIYA), SCOPE));
		assertEquals(BrainMemoryRecall.PEOPLE, BrainMemoryRecall.bucket(fact("m4", PRIYA, ACME), SCOPE));
		assertEquals(BrainMemoryRecall.TOPICS, BrainMemoryRecall.bucket(fact("m5", ACME), SCOPE));
		assertEquals(BrainMemoryRecall.TOPICS, BrainMemoryRecall.bucket(fact("m6", ACME_ACCOUNT), SCOPE));
		// about someone or something not on this thread
		assertNull(BrainMemoryRecall.bucket(fact("m7", OTHER_THREAD), SCOPE));
		assertNull(BrainMemoryRecall.bucket(fact("m8", new Ref(BrainMemoryUtils.PERSON, "p-elsewhere")), SCOPE));
		// a kept-out thread loses its own memories
		Scope keptOut = new Scope("th-1", Set.of(), Set.of("t-acme"), Set.of(), true, Set.of());
		assertNull(BrainMemoryRecall.bucket(fact("m9", THIS_THREAD), keptOut));
		assertEquals(BrainMemoryRecall.TOPICS, BrainMemoryRecall.bucket(fact("m10", THIS_THREAD, ACME), keptOut));
	}

	@Test
	void selectDropsExpiredExcludedAndOtherMemoriesThenOrdersThem() {
		Memory expired = new Memory("m-exp", BrainMemoryUtils.FACT, "On leave", BrainMemoryUtils.ACTIVE, "you", true,
				false, null, Timestamp.valueOf("2026-10-01 00:00:00"), Source.ui(), NOW, NOW, NOW, List.of());
		Memory aboutDana = fact("m-dana", DANA, ACME);
		Memory saidByDana = new Memory("m-said", BrainMemoryUtils.FACT, "Budget frozen", BrainMemoryUtils.ACTIVE,
				"you", true, false, null, null,
				new Source(BrainMemoryUtils.FROM_THREAD_FACT, "th-1", null, "f1", "p-dana", "Dana"), NOW, NOW, NOW,
				List.of(THIS_THREAD));
		Memory learnedTopic = memory("m-topic-learned", BrainMemoryUtils.FACT, "Renews in March", false,
				"2026-10-05 09:00:00", ACME);
		Memory confirmedTopic = memory("m-topic", BrainMemoryUtils.FACT, "Three quotes", true, "2026-09-01 09:00:00",
				ACME);
		Memory newerPeople = memory("m-people-new", BrainMemoryUtils.FACT, "Approves budgets", true,
				"2026-10-04 09:00:00", PRIYA);
		Memory olderPeople = memory("m-people-old", BrainMemoryUtils.FACT, "Prefers calls", true,
				"2026-09-04 09:00:00", PRIYA);
		Memory preference = memory("m-pref", BrainMemoryUtils.PREFERENCE, "Sign as Rob", true, "2026-08-01 09:00:00");
		Memory superseded = new Memory("m-old", BrainMemoryUtils.FACT, "Old", BrainMemoryUtils.SUPERSEDED, "you",
				true, false, null, null, Source.ui(), NOW, NOW, NOW, List.of());

		List<Line> lines = BrainMemoryRecall.select(List.of(expired, aboutDana, saidByDana, learnedTopic,
				confirmedTopic, newerPeople, olderPeople, preference, superseded, fact("m-other", OTHER_THREAD)), SCOPE,
				NOW);
		assertEquals(List.of("m-pref", "m-people-new", "m-people-old", "m-topic", "m-topic-learned"),
				lines.stream().map(line -> line.memory().id()).toList());
	}

	@Test
	void budgetStopsAtTheCapAndLetsShortMemoriesPastALongOne() {
		List<Line> lines = new ArrayList<>();
		lines.add(new Line(memory("long", BrainMemoryUtils.FACT, "x".repeat(400), true, "2026-10-01 09:00:00"),
				BrainMemoryRecall.GENERAL));
		lines.add(new Line(memory("short", BrainMemoryUtils.FACT, "short", true, "2026-10-01 09:00:00"),
				BrainMemoryRecall.GENERAL));
		assertEquals(List.of("short"),
				BrainMemoryRecall.budget(lines, 300).stream().map(line -> line.memory().id()).toList());

		List<Line> many = new ArrayList<>();
		for (int i = 0; i < BrainMemoryRecall.MAX_MEMORIES + 5; i++) {
			many.add(new Line(fact("m" + i), BrainMemoryRecall.GENERAL));
		}
		assertEquals(BrainMemoryRecall.MAX_MEMORIES, BrainMemoryRecall.budget(many, 1_000_000).size());
	}

	@Test
	void renderSplitsConfirmedFromLearnedAndSaysWhatDidNotFit() {
		Memory preference = memory("pref1", BrainMemoryUtils.PREFERENCE, "Sign emails as Rob", true,
				"2026-08-01 09:00:00");
		Memory onThread = memory("thr1", BrainMemoryUtils.FACT, "Vendor agreed to 10% off", true,
				"2026-09-01 09:00:00", THIS_THREAD, PRIYA);
		Memory learned = new Memory("lrn1", BrainMemoryUtils.FACT, "Dana is out until November",
				BrainMemoryUtils.ACTIVE, BrainMemoryUtils.ASSISTANT, false, false, null,
				Timestamp.valueOf("2026-11-01 00:00:00"), Source.ui(), NOW, Timestamp.valueOf("2026-10-03 16:00:00"),
				null, List.of(DANA));
		String block = BrainMemoryRecall.render(new Recall(List.of(new Line(preference, BrainMemoryRecall.PREFERENCES),
				new Line(onThread, BrainMemoryRecall.THIS_THREAD), new Line(learned, BrainMemoryRecall.PEOPLE)), 2,
				Map.of(PRIYA, "Priya Shah", DANA, "Dana Lee")));

		assertTrue(block.startsWith("## Memory"));
		assertTrue(block.contains("- [m:pref1] Sign emails as Rob (preference)"));
		assertTrue(block.contains("- [m:thr1] Vendor agreed to 10% off (about this thread, Priya Shah)"));
		assertTrue(block.contains(
				"- [m:lrn1] Dana is out until November (about Dana Lee; until 2026-11-01; saved in chat 2026-10-03)"));
		assertTrue(block.indexOf("Confirmed by the owner:") < block.indexOf("[m:pref1]"));
		assertTrue(block.indexOf("Learned, not confirmed") < block.indexOf("[m:lrn1]"));
		assertTrue(block.indexOf("[m:thr1]") < block.indexOf("Learned, not confirmed"));
		assertTrue(block.endsWith("2 more memories apply but did not fit; use SearchMemories to find them."));
		assertFalse(block.chars().anyMatch(c -> c > 127), "prompt text stays ASCII");

		String empty = BrainMemoryRecall.render(new Recall(List.of(), 0, Map.of()));
		assertTrue(empty.contains("Nothing yet."));
		assertFalse(empty.contains("Confirmed by the owner"));
	}

	@Test
	void aChannelOrTopicRuleWithoutAPersonKeepsTheThreadOut() {
		Rule channel = new Rule("r1", "exclude_channel", null, null, null, "outlook");
		Rule topicByValue = new Rule("r2", "exclude_topic", "t-secret", null, null, null);
		Rule personOnChannel = new Rule("r3", "exclude_channel", null, null, "p-priya", "outlook");
		assertTrue(BrainMemoryRecall.keptOut(List.of(channel), "outlook", List.of()));
		assertFalse(BrainMemoryRecall.keptOut(List.of(channel), "teams", List.of()));
		assertTrue(BrainMemoryRecall.keptOut(List.of(topicByValue), "outlook", List.of("t-secret")));
		assertFalse(BrainMemoryRecall.keptOut(List.of(personOnChannel), "outlook", List.of()));
	}
}
