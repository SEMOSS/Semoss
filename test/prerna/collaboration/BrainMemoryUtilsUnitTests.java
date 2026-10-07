package prerna.collaboration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import prerna.collaboration.BrainMemoryUtils.Memory;
import prerna.collaboration.BrainMemoryUtils.Ref;
import prerna.collaboration.BrainMemoryUtils.Source;

class BrainMemoryUtilsUnitTests {

	private static final Ref PRIYA = new Ref(BrainMemoryUtils.PERSON, "p-priya");
	private static final Ref DANA = new Ref(BrainMemoryUtils.PERSON, "p-dana");
	private static final Ref ACME = new Ref(BrainMemoryUtils.TOPIC, "t-acme");

	private static Memory memory(String id, String text, boolean confirmed, String updated, Ref... about) {
		Timestamp at = Timestamp.valueOf(updated);
		return new Memory(id, BrainMemoryUtils.FACT, text, BrainMemoryUtils.ACTIVE, BrainMemoryUtils.ASSISTANT,
				confirmed, false, null, null, Source.ui(), at, at, null, List.of(about));
	}

	// ---- text ----

	@Test
	void textIsOneTrimmedLine() {
		assertEquals("Priya approves Acme budgets", BrainMemoryUtils.cleanText("  Priya approves\n\tAcme   budgets "));
	}

	@Test
	void emptyOrLongTextIsRefused() {
		assertThrows(IllegalArgumentException.class, () -> BrainMemoryUtils.cleanText("   "));
		assertThrows(IllegalArgumentException.class, () -> BrainMemoryUtils.cleanText(null));
		assertThrows(IllegalArgumentException.class,
				() -> BrainMemoryUtils.cleanText("x".repeat(BrainMemoryUtils.MAX_CHARS + 1)));
		assertEquals(BrainMemoryUtils.MAX_CHARS,
				BrainMemoryUtils.cleanText("x".repeat(BrainMemoryUtils.MAX_CHARS)).length());
	}


	@Test
	void ordinaryTextIsNotASecret() {
		assertFalse(BrainMemoryUtils.looksSecret("The secret to getting Dana's attention is a short subject line"));
		assertFalse(BrainMemoryUtils.looksSecret("Reset passwords through the IT portal, never by email"));
		assertFalse(BrainMemoryUtils.looksSecret("The project id is 4f9c2e1a-1b2c-4d5e-8f90-1234567890ab"));
		assertFalse(BrainMemoryUtils.looksSecret("Priya Shah is the final approver on Acme budgets"));
	}

	@Test
	void kindFallsBackOrIsChecked() {
		assertEquals(BrainMemoryUtils.FACT, BrainMemoryUtils.kind(null, BrainMemoryUtils.FACT));
		assertEquals(BrainMemoryUtils.PREFERENCE, BrainMemoryUtils.kind(" Preference ", null));
		assertThrows(IllegalArgumentException.class, () -> BrainMemoryUtils.kind(null, null));
		assertThrows(IllegalArgumentException.class, () -> BrainMemoryUtils.kind("rule", BrainMemoryUtils.FACT));
	}

	// ---- links ----

	@Test
	void refsParseFromMapsAndText() {
		assertEquals(PRIYA, BrainMemoryUtils.parseRef(Map.of("type", "person", "id", "p-priya")));
		assertEquals(ACME, BrainMemoryUtils.parseRef("Topic:t-acme"));
		assertNull(BrainMemoryUtils.parseRef(null));
		assertNull(BrainMemoryUtils.parseRef(" "));
		assertThrows(IllegalArgumentException.class, () -> BrainMemoryUtils.parseRef("p-priya"));
		assertThrows(IllegalArgumentException.class, () -> BrainMemoryUtils.parseRef(Map.of("type", "team", "id", "x")));
		assertThrows(IllegalArgumentException.class, () -> BrainMemoryUtils.parseRef(Map.of("type", "person")));
	}

	@Test
	void sessionThreadLinksAreDroppedWithoutALookup() {
		// a lookup would need the database; a session thread never reaches one
		assertEquals(List.of(), BrainMemoryUtils.refs("owner", "NATIVE",
				List.of(Map.of("type", "thread", "id", BrainMemoryUtils.SESSION_THREAD_PREFIX + "abc"))));
		assertEquals(List.of(), BrainMemoryUtils.refs("owner", "NATIVE", null));
	}

	// ---- matching ----

	@Test
	void alikeNeedsTheSameWordsAndALinkInCommon() {
		Memory onPriya = memory("m1", "Priya Shah approves the Acme budget", false, "2026-10-01 10:00:00", PRIYA);
		List<Memory> all = List.of(onPriya);
		assertSame(onPriya, BrainMemoryUtils.alike(all, "priya shah approves the acme budget.", List.of(PRIYA, ACME)));
		// the same sentence about someone else is a different memory
		assertNull(BrainMemoryUtils.alike(all, "Priya Shah approves the Acme budget", List.of(DANA)));
		// an unlinked memory matches a linked one with the same words
		assertSame(onPriya, BrainMemoryUtils.alike(all, "Priya Shah approves the Acme budget", List.of()));
		assertNull(BrainMemoryUtils.alike(all, "Dana books the travel", List.of(PRIYA)));
	}

	@Test
	void rankUsesWordsInTextAndLinkNamesBestFirst() {
		Memory budget = memory("m1", "Approves every budget over 50k", false, "2026-10-01 10:00:00", PRIYA);
		Memory travel = memory("m2", "Books travel through Concur", false, "2026-10-02 10:00:00", DANA);
		Memory both = memory("m3", "Priya wants budgets before travel is booked", true, "2026-09-01 10:00:00");
		Map<Ref, String> names = Map.of(PRIYA, "Priya Shah", DANA, "Dana Lee");

		List<Memory> ranked = BrainMemoryUtils.rank(List.of(budget, travel, both), "Priya budgets", names);
		// both words: m1 through its link name and plural, m3 in its text; m3 is confirmed so it leads the tie
		assertEquals(List.of("m3", "m1"), ranked.stream().map(Memory::id).toList());
		assertEquals(List.of("m2"),
				BrainMemoryUtils.rank(List.of(budget, travel, both), "what about Dana?", names).stream()
						.map(Memory::id).toList());
		assertEquals(List.of(), BrainMemoryUtils.rank(List.of(budget, travel), "the and of", names));
	}

	@Test
	void termsDropStopWordsAndPlurals() {
		assertEquals(Set.of("budget", "acme", "class"), BrainMemoryUtils.terms("The budgets of Acme, class"));
		assertEquals(0.5, BrainMemoryUtils.score(BrainMemoryUtils.terms("budget calls"), "Budgets are due", List.of()));
	}

	// ---- privacy ----

	@Test
	void blockedWhenAboutOrSaidByAnExcludedPerson() {
		Memory aboutPriya = memory("m1", "Prefers calls", false, "2026-10-01 10:00:00", PRIYA);
		Memory saidByPriya = new Memory("m2", BrainMemoryUtils.FACT, "Budget is frozen", BrainMemoryUtils.ACTIVE,
				BrainProfileUtils.YOU, true, false, null, null,
				new Source(BrainMemoryUtils.FROM_THREAD_FACT, "th1", null, "f1", "p-priya", "Priya"), null, null, null,
				List.of(ACME));
		Memory unrelated = memory("m3", "Dana books travel", false, "2026-10-01 10:00:00", DANA);
		Set<String> excluded = Set.of("p-priya");
		assertTrue(BrainMemoryUtils.blocked(aboutPriya, excluded));
		assertTrue(BrainMemoryUtils.blocked(saidByPriya, excluded));
		assertFalse(BrainMemoryUtils.blocked(unrelated, excluded));
		assertFalse(BrainMemoryUtils.blocked(aboutPriya, Set.of()));
	}

	@Test
	void ownerWroteAndExpiry() {
		Memory learned = memory("m1", "Prefers calls", false, "2026-10-01 10:00:00");
		Memory confirmed = memory("m2", "Prefers calls", true, "2026-10-01 10:00:00");
		assertFalse(learned.ownerWrote());
		assertTrue(confirmed.ownerWrote());
		Memory typed = new Memory("m3", BrainMemoryUtils.PREFERENCE, "Sign as Rob", BrainMemoryUtils.SUGGESTED,
				BrainProfileUtils.YOU, false, false, null, Timestamp.valueOf("2026-10-05 00:00:00"), Source.ui(), null,
				null, null, List.of());
		assertTrue(typed.ownerWrote());
		assertTrue(typed.expired(Timestamp.valueOf("2026-10-05 00:00:00")));
		assertFalse(typed.expired(Timestamp.valueOf("2026-10-04 23:59:59")));
		assertFalse(learned.expired(Timestamp.valueOf("2030-01-01 00:00:00")));
	}

	// ---- ids ----

	@Test
	void shortIdsAreTwelveCharactersFromTheAlphabet() {
		String id = BrainMemoryUtils.shortId(0x0123456789ABCDEFL);
		assertEquals(12, id.length());
		assertTrue(id.matches("[abcdefghijkmnpqrstuvwxyz23456789]{12}"));
		assertNotEquals(id, BrainMemoryUtils.shortId(0x0123456789ABCDEEL ^ 0x10L));
	}

	@Test
	void stableIdsRepeatForTheSameSourceRow() {
		String first = BrainMemoryUtils.stableId("owner", "NATIVE", "memory", "topic_note", "n1");
		assertEquals(first, BrainMemoryUtils.stableId("owner", "NATIVE", "memory", "topic_note", "n1"));
		assertNotEquals(first, BrainMemoryUtils.stableId("owner", "NATIVE", "memory", "thread_fact", "n1"));
		assertNotEquals(first, BrainMemoryUtils.stableId("other", "NATIVE", "memory", "topic_note", "n1"));
	}
}
