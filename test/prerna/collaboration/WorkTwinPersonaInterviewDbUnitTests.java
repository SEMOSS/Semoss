/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.collaboration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.javatuples.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.SystemEngineRegistry;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

/**
 * The interview that writes a persona, against an in-memory H2 Collaboration schema, with a model and the owner's
 * places stood in for: what it reads and keeps, the order of its steps, what it does when the model cannot be read,
 * and what the persona it writes is made of.
 */
class WorkTwinPersonaInterviewDbUnitTests {

	private Connection connection;
	private MockedStatic<SystemEngineRegistry> registry;
	private User user;
	private String ownerId;
	private String ownerType;

	/** What the model answers each kind of call with; a call with no answer here cannot be read. */
	private final Map<String, Object> canned = new HashMap<>();
	/** The calls made, by kind, and what each was given. */
	private final List<String> asked = new ArrayList<>();
	private final List<Map<String, Object>> given = new ArrayList<>();
	private final WorkTwinPersonaInterview.Model model = (input, instructions, schema, maxTokens, key) -> {
		// the replies to choose between and the twin's own answers are both asked for as rounds, and the persona by
		// the first of its sections
		String kind = "rounds".equals(key) ? (instructions.contains("\"aIs\"") ? "voice" : "mirror")
				: "voice".equals(key) ? "persona" : key;
		asked.add(kind);
		given.add(input);
		Object answer = canned.get(kind);
		if (answer == null) {
			throw new IllegalArgumentException("The model's answer could not be read. Try again.");
		}
		@SuppressWarnings("unchecked")
		Map<String, Object> read = (Map<String, Object>) answer;
		return read;
	};

	private List<String> mail = List.of();
	private List<String> teams = List.of();
	private List<String> memories = List.of();
	private boolean teamsDown;
	private final List<String> read = new ArrayList<>();
	private final WorkTwinPersonaInterview.Places places = new WorkTwinPersonaInterview.Places() {
		@Override
		public List<String> mail() {
			read.add("mail");
			return mail;
		}

		@Override
		public List<String> teams() {
			read.add("teams");
			if (teamsDown) {
				throw new IllegalStateException("Graph returned HTTP 403 Forbidden");
			}
			return teams;
		}

		@Override
		public List<String> memories() {
			read.add("memory");
			return memories;
		}

		@Override
		public String style() {
			return "Short. Signs with first name.";
		}
	};

	@BeforeEach
	void setUp() throws Exception {
		connection = DriverManager.getConnection("jdbc:h2:mem:collab_interview_" + UUID.randomUUID());
		AbstractSqlQueryUtil queryUtil = SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB);
		IRDBMSEngine engine = mock(IRDBMSEngine.class);
		when(engine.getConnection()).thenReturn(connection);
		when(engine.getQueryUtil()).thenReturn(queryUtil);
		when(engine.getPreparedStatement(anyString()))
				.thenAnswer(call -> connection.prepareStatement(call.getArgument(0)));
		registry = mockStatic(SystemEngineRegistry.class);
		registry.when(SystemEngineRegistry::getCollaborationDb).thenReturn(engine);
		try (Statement statement = connection.createStatement()) {
			for (Pair<String, List<Pair<String, String>>> table : new CollaborationOwlCreator(queryUtil)
					.getDBSchema()) {
				List<String> columns = new ArrayList<>();
				for (Pair<String, String> column : table.getValue1()) {
					// quoted as the server quotes them: H2 2.x reserves words such as VALUE
					String name = column.getValue0();
					columns.add((queryUtil.isSelectorKeyword(name) ? queryUtil.getEscapeKeyword(name) : name) + " "
							+ column.getValue1());
				}
				statement.execute("CREATE TABLE " + table.getValue0() + " (" + String.join(", ", columns) + ")");
			}
		}
		AccessToken token = new AccessToken();
		token.setId("ownerid");
		token.setName("Owner");
		token.setEmail("owner@test.com");
		token.setProvider(AuthProvider.NATIVE);
		user = new User();
		user.setAccessToken(token);
		Pair<String, String> owner = User.getPrimaryUserIdAndTypePair(user);
		ownerId = owner.getValue0();
		ownerType = owner.getValue1();

		// a model that answers everything, until a test takes an answer away
		canned.put("observations", Map.of("observations", List.of("You answer in one or two lines."), "dials",
				Map.of("formal", 4, "brief", 1, "blunt", 2, "playful", 9)));
		canned.put("voice", Map.of("rounds", List.of(
				round("Sam, a teammate", "Can we move the date?", "No.", "blunt", "Let us look at it together.", "soft"),
				round("Priya, your manager", "Can you take this on?", "Yes.", "short", "Yes, happily, tell me more.",
						"warm"),
				// not a choice: both replies are the same
				round("Dana, a peer", "Thoughts?", "Fine.", "plain", "Fine.", "plain"))));
		canned.put("message", Map.of("from", "Sam, a teammate", "message", "On time with the bug, or late without?"));
		canned.put("questions", Map.of("questions", List.of("When will it be done?", "Who owns this?")));
		canned.put("probes", Map.of("probes",
				List.of(Map.of("question", "You set Blunt but picked the softer reply.", "a", "As blunt as the dial",
						"b", "Softer, like my picks"))));
		canned.put("mirror", Map.of("rounds", List.of(Map.of("ask", "Should we add the export?", "reply",
				"Not before launch. Scope is frozen. Happy to discuss further!"),
				Map.of("ask", "What is the budget?", "reply", "That is not mine to say. Ask Dana Smith."))));
		canned.put("reply", Map.of("reply", "Not before launch. Scope is frozen."));
	}

	@AfterEach
	void tearDown() throws Exception {
		registry.close();
		connection.close();
	}

	// ---- starting and reading ----

	@Test
	void thereIsNoInterviewUntilItIsStartedAndNoneWithoutAModel() throws Exception {
		assertEquals(Map.of("phase", "none"), call(null, null, null));
		assertNull(WorkTwinPersona.get(user).get("interview"));
		IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
				() -> WorkTwinPersonaInterview.step(user, null, null, false, List.of("mail"), null, places, false));
		assertTrue(refused.getMessage().contains("can still be written by hand"), refused.getMessage());
		assertThrows(IllegalArgumentException.class, () -> call(null, null, List.of("mail", "their-calendar")));
		assertEquals(0, count("BRAIN_PERSONA"));
	}

	@Test
	void itReadsOnlyThePlacesAllowedAndKeepsNotesNeverWhatWasRead() throws Exception {
		mail = List.of("Ship it Friday. PASSWORD-123 is the vendor login.", "x".repeat(5000), " ");
		memories = List.of("Prefers bullet points.");
		Map<String, Object> first = call(null, null, List.of("mail", "memory"));

		assertEquals(List.of("mail", "memory"), read);
		assertEquals("asking", first.get("phase"));
		assertEquals("confirm", step(first).get("id"));
		assertEquals(List.of("You answer in one or two lines."), step(first).get("items"));
		// the model was given the samples, each cut to size and named for where it is from, and the email style
		Map<String, Object> input = given.get(0);
		List<?> samples = (List<?>) input.get("samples");
		assertEquals(3, samples.size());
		assertEquals(Map.of("from", "mail they sent", "text", "Ship it Friday. PASSWORD-123 is the vendor login."),
				samples.get(0));
		assertEquals(WorkTwinPersonaInterview.MAX_SAMPLE, ((Map<?, ?>) samples.get(1)).get("text").toString().length());
		assertEquals("a note the assistant keeps of how they like things done", ((Map<?, ?>) samples.get(2)).get("from"));
		assertEquals("Short. Signs with first name.", input.get("style"));
		// what is kept is the notes: nothing that was read
		String stored = text("INTERVIEW_JSON");
		assertFalse(stored.contains("PASSWORD-123") || stored.contains("bullet points"), stored);
		assertTrue(stored.contains("You answer in one or two lines."), stored);
		// the dials open where the reading put them, the middle for a guess that is no stop
		Map<String, Object> dials = call(answer("confirm", Map.of("items", List.of("You answer in one line."))), null,
				null);
		assertEquals(List.of(4, 1, 2, 3),
				((List<?>) step(dials).get("dials")).stream().map(dial -> ((Map<?, ?>) dial).get("value")).toList());
		// going on without touching them keeps them there, and one that was moved is where it was put
		call(answer("dials", Map.of("values", Map.of("brief", 5))), null, null);
		assertEquals(Map.of("formal", 4, "brief", 5, "blunt", 2, "playful", 3), given.get(given.size() - 1).get("dials"));
		assertEquals(List.of("You answer in one line."), given.get(given.size() - 1).get("observations"));
	}

	@Test
	void goingOnWithoutTouchingWhatWasNoticedKeepsIt() {
		mail = List.of("Ship it Friday, no later.");
		call(null, null, List.of("mail"));
		call(answer("confirm", Map.of()), null, null);
		call(answer("dials", Map.of()), null, null);
		assertEquals(List.of("You answer in one or two lines."), given.get(given.size() - 1).get("observations"));
		// nothing kept is an answer too
		call(null, null, List.of("mail"));
		call(answer("confirm", Map.of("items", List.of())), null, null);
		call(answer("dials", Map.of()), null, null);
		assertEquals(List.of(), given.get(given.size() - 1).get("observations"));
	}

	@Test
	void aPlaceThatCannotBeReadIsNamedAndTheRestIsReadWithoutIt() {
		teamsDown = true;
		mail = List.of("Ship it Friday, no later.");
		Map<String, Object> first = call(null, null, List.of("mail", "teams"));
		assertEquals("confirm", step(first).get("id"));
		assertEquals(List.of("Your Teams messages could not be read: Graph returned HTTP 403 Forbidden"),
				first.get("notes"));

		// nothing found anywhere, or notes the model could not write: the questions start without them
		mail = List.of();
		teamsDown = false;
		Map<String, Object> cold = call(null, null, List.of("teams"));
		assertEquals("dials", step(cold).get("id"));
		assertEquals(List.of("I found no Teams messages of yours to read."), cold.get("notes"));
		teams = List.of("Sounds good, send it over.");
		canned.remove("observations");
		Map<String, Object> unread = call(null, null, List.of("teams"));
		assertEquals("dials", step(unread).get("id"));
		assertTrue(unread.get("notes").toString().contains("could not be turned into notes"));
	}

	@Test
	void oneCallReadsNoMoreThanItsBudget() {
		mail = new ArrayList<>();
		for (int i = 0; i < 80; i++) {
			mail.add(("mail " + i + " ").repeat(200));
		}
		call(null, null, List.of("mail"));
		int total = ((List<?>) given.get(0).get("samples")).stream()
				.mapToInt(sample -> ((Map<?, ?>) sample).get("text").toString().length()).sum();
		assertTrue(total <= WorkTwinPersonaInterview.MAX_READ && total > WorkTwinPersonaInterview.MAX_READ - 600,
				String.valueOf(total));
	}

	// ---- the steps ----

	@Test
	void itAsksItsStepsInOrderAndWritesThePersonaFromTheAnswers() throws Exception {
		// read nothing: the questions start at the dials
		Map<String, Object> dials = call(null, null, List.of("none"));
		assertEquals(List.of(), read);
		assertEquals("dials", step(dials).get("id"));
		assertEquals(Map.of("answered", 0, "of", 10), dials.get("progress"));
		assertEquals("Writing replies you might send", step(dials).get("wait"));

		Map<String, Object> voice = call(
				answer("dials", Map.of("values", Map.of("formal", 2, "brief", 1, "blunt", 5, "playful", 77))), null,
				null);
		assertEquals("voice", step(voice).get("id"));
		assertEquals(List.of("formal", "brief", "diplomatic"), voice.get("learned"));
		// two rounds were a choice and are kept, each with two replies
		List<?> rounds = (List<?>) step(voice).get("rounds");
		assertEquals(List.of("r1", "r2"), rounds.stream().map(round -> ((Map<?, ?>) round).get("id")).toList());
		assertTrue(rounds.stream().allMatch(round -> ((Map<?, ?>) round).containsKey("a")), rounds.toString());
		assertEquals(Map.of("formal", 2, "brief", 1, "blunt", 5, "playful", 3), given.get(0).get("dials"));

		Map<String, Object> limits = call(answer("voice", Map.of("rounds", Map.of("r1", Map.of("pick", "a"), "r2",
				Map.of("pick", "own", "text", "Yes, if the audit\n moves.")))), null, null);
		assertEquals("limits", step(limits).get("id"));
		assertTrue(((List<?>) limits.get("learned")).contains("blunt"), limits.get("learned").toString());
		assertFalse(step(limits).get("options").toString().contains("personal"), step(limits).toString());

		// only what was offered counts as picked; what the owner added is theirs
		Map<String, Object> checkpoint = call(answer("limits", Map.of("picked",
				List.of("Budgets and pricing", "Budgets and pricing", "Not an option"), "own", List.of("Vendor names"))),
				null, null);
		assertEquals("priorities", step(checkpoint).get("id"));
		assertEquals(true, checkpoint.get("checkpoint"));
		assertEquals(true, checkpoint.get("canWrap"));
		assertNull(limits.get("canWrap"));
		assertEquals(Map.of("answered", 3, "of", 10), checkpoint.get("progress"));

		// wrapped up here: the steps that go deeper are left out, and it looks for answers that disagree
		Map<String, Object> probe = call(null, true, null);
		assertEquals("probe", step(probe).get("id"));
		assertEquals(Map.of("answered", 3, "of", 5), probe.get("progress"));
		assertNull(probe.get("checkpoint"));
		assertNull(probe.get("canWrap"));

		Map<String, Object> mirror = call(answer("probe", Map.of("probes", Map.of("p1", Map.of("pick", "a")))), null,
				null);
		assertEquals("mirror", step(mirror).get("id"));
		assertEquals(2, ((List<?>) step(mirror).get("rounds")).size());

		// the model cannot be read for the persona yet: the call fails, and the answers are all still there
		assertThrows(IllegalArgumentException.class,
				() -> call(answer("mirror",
						Map.of("rounds",
								Map.of("m1", Map.of("verdict", "close", "flagged", List.of("Happy to discuss further!")),
										"m2", Map.of("verdict", "yes")))),
						null, null));
		assertEquals(Map.of("answered", 5, "of", 5, "wrapped", true, "done", false),
				WorkTwinPersona.get(user).get("interview"));
		canned.put("persona", Map.of("short", "Direct and brief.", "voice", "# Voice\n- Leads with the answer.",
				"decide", "", "people", ""));
		Map<String, Object> written = call(null, null, null);
		assertEquals("done", written.get("phase"));
		@SuppressWarnings("unchecked")
		Map<String, String> sections = (Map<String, String>) ((Map<?, ?>) written.get("brief")).get("sections");
		assertEquals("Direct and brief.", sections.get("short"));
		// a model's line that could pass for a heading is cut as a save would cut it
		assertEquals("Voice\n- Leads with the answer.", sections.get("voice"));
		// what a twin must leave alone is the owner's own words
		assertEquals("Stay out of: Budgets and pricing; Vendor names.", sections.get("limits"));
		// their own reply first, then the reply they chose, then the twin's answer they said sounds like them
		assertEquals("Asked: Can you take this on?\nReply: Yes, if the audit moves.\n\n"
				+ "Asked: Can we move the date?\nReply: No.\n\n"
				+ "Asked: What is the budget?\nReply: That is not mine to say. Ask Dana Smith.", sections.get("examples"));
		assertEquals("", sections.get("stands"));
		// the persona their twins use is not touched: the result waits for the owner to save it
		assertEquals(false, WorkTwinPersona.get(user).get("exists"));
		assertEquals(Map.of("answered", 5, "of", 5, "wrapped", true, "done", true),
				WorkTwinPersona.get(user).get("interview"));
		// the model that wrote it was told what the owner marked as not them
		Map<String, Object> told = given.get(given.size() - 1);
		assertTrue(told.get("twinTried").toString().contains("Happy to discuss further!"), told.toString());
		assertTrue(told.get("settled").toString().contains("As blunt as the dial"), told.toString());
	}

	@Test
	void theWaitingStepIsReturnedAgainWithoutBeingWrittenAgain() {
		call(null, null, List.of("none"));
		call(answer("dials", Map.of("values", Map.of())), null, null);
		int calls = asked.size();
		Map<String, Object> again = call(null, null, null);
		assertEquals("voice", step(again).get("id"));
		// an answer sent twice, as after a call that timed out, is kept once and answers with what waits now
		assertEquals("voice", step(call(answer("dials", Map.of("values", Map.of("formal", 5))), null, null)).get("id"));
		assertEquals(calls, asked.size());
		assertEquals(List.of(), again.get("learned"));
		// an answer for a step that is neither waiting nor answered is refused, and nothing changes
		assertThrows(IllegalArgumentException.class, () -> call(answer("mirror", Map.of()), null, null));
		assertEquals("voice", step(call(null, null, null)).get("id"));
	}

	@Test
	void aStepCanBeSkippedAndAnAnswerIsKeptToWhatWasAsked() throws Exception {
		call(null, null, List.of("none"));
		call(answer("dials", Map.of("skipped", true)), null, null);
		call(answer("voice", Map.of("rounds",
				Map.of("r1", Map.of("pick", "c", "text", " "), "r2", Map.of("pick", "b")))), null, null);
		call(answer("limits", Map.of("picked", "Hiring", "own", List.of("x".repeat(500), 7, Map.of()))), null, null);
		String stored = text("INTERVIEW_JSON");
		// only the round that was answered has a pick, and only text of a size is kept
		assertEquals(1, stored.split("\"pick\"", -1).length - 1, stored);
		assertFalse(stored.contains("x".repeat(161)), stored);
		assertTrue(stored.contains("x".repeat(160)), stored);
	}

	@Test
	void whereTheModelCannotBeReadItAsksTheStandardStepsAndLeavesOutTheRest() {
		canned.clear();
		call(null, null, List.of("none"));
		Map<String, Object> voice = step(call(answer("dials", Map.of("values", Map.of())), null, null));
		assertEquals(4, ((List<?>) voice.get("rounds")).size());
		assertEquals("Can we push the deadline a week?", ((Map<?, ?>) ((List<?>) voice.get("rounds")).get(0)).get("message"));
		call(answer("voice", Map.of("rounds", Map.of("r1", Map.of("pick", "a")))), null, null);
		call(answer("limits", Map.of()), null, null);
		Map<String, Object> deeper = call(answer("priorities", Map.of("picked", List.of("Quality", "Speed"))), null,
				null);
		// past the first of the steps that go deeper there is no checkpoint, and wrapping up is still offered
		assertNull(deeper.get("checkpoint"));
		assertEquals(true, deeper.get("canWrap"));
		Map<String, Object> tradeoff = step(deeper);
		assertEquals("tradeoff", tradeoff.get("id"));
		assertEquals(1, ((List<?>) tradeoff.get("rounds")).size());
		Map<String, Object> stands = step(call(answer("tradeoff", Map.of("skipped", true)), null, null));
		assertEquals(5, ((List<?>) stands.get("suggestions")).size());
		call(answer("stands", Map.of("rows", List.of(Map.of("question", "When will it be done?", "answer",
				"When it is tested.")))), null, null);
		call(answer("review", Map.of("picked", List.of("Numbers have a source"))), null, null);
		// nothing was found to disagree and no answers could be written as them: both are left out, and it says so.
		// The persona cannot be written either, which fails the call and loses nothing
		assertThrows(IllegalArgumentException.class, () -> call(answer("people", Map.of()), null, null));
		canned.put("persona", Map.of("short", "", "voice", "- Plain.", "decide", "- Quality before speed.", "people",
				""));
		Map<String, Object> end = call(null, null, null);
		assertEquals("done", end.get("phase"));
		assertTrue(end.get("notes").toString().contains("could not write sample answers"), end.get("notes").toString());
		assertEquals(Map.of("answered", 10, "of", 10), end.get("progress"));
		@SuppressWarnings("unchecked")
		Map<String, String> sections = (Map<String, String>) ((Map<?, ?>) end.get("brief")).get("sections");
		assertEquals("If asked: When will it be done?\nSay: When it is tested.", sections.get("stands"));
		assertEquals("- Numbers have a source", sections.get("review"));
		assertEquals("", sections.get("limits"));
	}

	@Test
	void anAnswerInTheOwnersVoiceIsWrittenAgainFromWhatTheyMarkedAtMostTwice() {
		reachMirror();
		Map<String, Object> retry = new LinkedHashMap<>();
		retry.put("stepId", "mirror");
		retry.put("retry", "m1");
		retry.put("flagged", List.of("Happy to discuss further!"));
		retry.put("note", "Too chatty");
		Map<String, Object> again = call(retry, null, null);
		Map<?, ?> first = (Map<?, ?>) ((List<?>) step(again).get("rounds")).get(0);
		assertEquals("Not before launch. Scope is frozen.", first.get("reply"));
		assertEquals(1, ((Number) first.get("tries")).intValue());
		Map<String, Object> told = given.get(given.size() - 1);
		assertEquals(List.of("Happy to discuss further!"), told.get("notMe"));
		assertEquals("Too chatty", told.get("note"));

		call(retry, null, null);
		IllegalArgumentException enough = assertThrows(IllegalArgumentException.class, () -> call(retry, null, null));
		assertTrue(enough.getMessage().contains("twice already"), enough.getMessage());
		// one that cannot be written again says so and keeps the answer there was
		retry.put("retry", "m2");
		canned.remove("reply");
		assertThrows(IllegalArgumentException.class, () -> call(retry, null, null));
		assertEquals("That is not mine to say. Ask Dana Smith.",
				((Map<?, ?>) ((List<?>) step(call(null, null, null)).get("rounds")).get(1)).get("reply"));
	}

	@Test
	void aVerdictOnWordingThatWasSinceWrittenAgainIsNotKept() throws Exception {
		reachMirror();
		Map<String, Object> retry = new LinkedHashMap<>();
		retry.put("stepId", "mirror");
		retry.put("retry", "m1");
		retry.put("flagged", List.of("Happy to discuss further!"));
		call(retry, null, null);
		canned.put("persona", Map.of("short", "Direct.", "voice", "", "decide", "", "people", ""));
		// the page still holds what was said of the first wording of m1; m2 reads as it was judged
		assertEquals("done", call(answer("mirror", Map.of("rounds", Map.of("m1",
				Map.of("verdict", "no", "flagged", List.of("Happy to discuss further!"), "tries", 0), "m2",
				Map.of("verdict", "yes", "tries", 0)))), null, null).get("phase"));
		String stored = text("INTERVIEW_JSON");
		assertEquals(1, stored.split("\"verdict\"", -1).length - 1, stored);
		assertTrue(stored.contains("\"verdict\":\"yes\""), stored);
	}

	@Test
	void anInterviewThatWasWrappedUpGoesOnWhereItLeftOff() {
		reachMirror();
		canned.put("persona", Map.of("short", "Direct.", "voice", "", "decide", "", "people", ""));
		assertEquals("done", call(answer("mirror", Map.of()), null, null).get("phase"));

		Map<String, Object> more = call(null, false, null);
		assertEquals("priorities", step(more).get("id"));
		// its persona and its closing answers are from before the new answers, so they go
		assertEquals(Map.of("answered", 3, "of", 10, "wrapped", false, "done", false),
				WorkTwinPersona.get(user).get("interview"));
		// wrapping up again while a deeper step waits drops that step and closes with fresh checks: what was asked
		// at the close before is not shown again
		int calls = asked.size();
		assertEquals("probe", step(call(null, true, null)).get("id"));
		assertEquals(calls + 1, asked.size());
	}

	// ---- going back ----

	@Test
	@SuppressWarnings("unchecked")
	void goingBackWaitsOnTheStepBeforeWithWhatWasSaidAndComesForwardInOrder() {
		Map<String, Object> dials = call(null, null, List.of("none"));
		// nothing is before the first step, so going back from it stays on it
		assertNull(dials.get("canBack"));
		assertEquals("dials", step(back()).get("id"));
		Map<String, Object> voice = call(answer("dials", Map.of("values", Map.of("formal", 5))), null, null);
		assertEquals(true, voice.get("canBack"));
		assertNull(voice.get("previous"));
		Map<String, Object> picks = Map.of("rounds",
				Map.of("r1", Map.of("pick", "a"), "r2", Map.of("pick", "own", "text", "Yes, later.")));
		assertEquals("limits", step(call(answer("voice", picks), null, null)).get("id"));
		int calls = asked.size();

		// back to the replies: the same ones, with what was picked, and the model is not asked again
		Map<String, Object> again = back();
		assertEquals("voice", step(again).get("id"));
		assertEquals(step(voice).get("rounds"), step(again).get("rounds"));
		assertEquals(picks, again.get("previous"));
		assertEquals(Map.of("answered", 1, "of", 10), again.get("progress"));
		// and back again, to the dials where the owner left them
		Map<String, Object> first = back();
		assertEquals("dials", step(first).get("id"));
		Map<String, Object> values = (Map<String, Object>) ((Map<String, Object>) first.get("previous")).get("values");
		assertEquals(5, ((Number) values.get("formal")).intValue());
		assertEquals(3, ((Number) values.get("brief")).intValue());
		assertNull(first.get("canBack"));

		// forward with nothing changed: the steps come round in order, each with its answer, to the one that waited
		Map<String, Object> second = call(answer("dials", (Map<String, Object>) first.get("previous")), null, null);
		assertEquals("voice", step(second).get("id"));
		assertEquals(picks, second.get("previous"));
		Map<String, Object> third = call(answer("voice", picks), null, null);
		assertEquals("limits", step(third).get("id"));
		assertNull(third.get("previous"));
		assertEquals(calls, asked.size());

		// the offer to wrap up is made once, before the first deeper step is answered
		Map<String, Object> checkpoint = call(answer("limits", Map.of("picked", List.of("Hiring"))), null, null);
		assertEquals(true, checkpoint.get("checkpoint"));
		assertEquals("tradeoff",
				step(call(answer("priorities", Map.of("picked", List.of("Quality", "Speed"))), null, null)).get("id"));
		Map<String, Object> priorities = back();
		assertEquals("priorities", step(priorities).get("id"));
		assertNull(priorities.get("checkpoint"));
		assertEquals(true, priorities.get("canWrap"));
		assertEquals(List.of("Quality", "Speed"), ((Map<?, ?>) priorities.get("previous")).get("picked"));
	}

	@Test
	@SuppressWarnings("unchecked")
	void anEarlierAnswerThatChangesHasTheClosingStepsWrittenAgain() {
		reachMirror();
		int calls = asked.size();
		// back past the check, which was answered, to what the twin stays out of
		Map<String, Object> probe = back();
		assertEquals("probe", step(probe).get("id"));
		assertEquals(Map.of("probes", Map.of()), probe.get("previous"));
		assertEquals("limits", step(back()).get("id"));

		// the same answer again: the check and the sample answers are the ones written before
		assertEquals("probe", step(call(answer("limits", Map.of()), null, null)).get("id"));
		Map<String, Object> mirror = call(answer("probe", Map.of()), null, null);
		assertEquals("mirror", step(mirror).get("id"));
		assertEquals(calls, asked.size());
		// all the way back to the dials and forward again as each was left, numbers and all: still nothing new
		back();
		back();
		Map<String, Object> voice = back();
		assertEquals("voice", step(voice).get("id"));
		Map<String, Object> dials = back();
		assertEquals("dials", step(dials).get("id"));
		call(answer("dials", (Map<String, Object>) dials.get("previous")), null, null);
		call(answer("voice", (Map<String, Object>) voice.get("previous")), null, null);
		call(answer("limits", Map.of()), null, null);
		assertEquals("mirror", step(call(answer("probe", Map.of()), null, null)).get("id"));
		assertEquals(calls, asked.size());

		// a different answer: both were written from the old one, so both are written again
		back();
		back();
		assertEquals("probe", step(call(answer("limits", Map.of("picked", List.of("Hiring"))), null, null)).get("id"));
		assertEquals(calls + 1, asked.size());
		assertEquals("probes", asked.get(asked.size() - 1));
		assertEquals("mirror", step(call(answer("probe", Map.of()), null, null)).get("id"));
		assertEquals(calls + 2, asked.size());

		// a check that found nothing was never put to the owner, so going back passes it
		canned.put("probes", Map.of("probes", List.of()));
		back();
		back();
		assertEquals("mirror", step(call(answer("limits", Map.of()), null, null)).get("id"));
		assertEquals("limits", step(back()).get("id"));
	}

	@Test
	void anInterviewThatWaitedOnAStepThatIsGoneGoesOnToTheNext() throws Exception {
		call(null, null, List.of("none"));
		String waiting = "{\"sources\":[],\"notes\":{\"observations\":[],\"dials\":{},\"skipped\":[]},"
				+ "\"answers\":{\"dials\":{\"values\":{}},\"voice\":{\"rounds\":[]},\"limits\":{\"picked\":[],\"own\":[]}},"
				+ "\"pending\":{\"id\":\"handoff\",\"kind\":\"handoff\",\"title\":\"Who should it send people to?\","
				+ "\"topics\":[]}}";
		try (PreparedStatement statement = connection.prepareStatement("UPDATE BRAIN_PERSONA SET INTERVIEW_JSON = ?")) {
			statement.setString(1, waiting);
			assertEquals(1, statement.executeUpdate());
		}
		Map<String, Object> next = call(null, null, null);
		assertEquals("priorities", step(next).get("id"));
		assertEquals(Map.of("answered", 3, "of", 10), next.get("progress"));
	}

	// ---- Teams ----

	@Test
	void fromTeamsItTakesOnlyWhatTheOwnerWroteAndNothingTheirRulesKeepOut() throws Exception {
		List<WorkTwinSuggest.RecentChat> chats = List.of(
				new WorkTwinSuggest.RecentChat("Dana Ortiz",
						List.of(hit("chat-dana", "me", "Send me the quote before lunch, please."),
								hit("chat-dana", "dana", "Here is the quote you asked for this morning."))),
				new WorkTwinSuggest.RecentChat("Team",
						List.of(hit("chat-team", "me", "ok"), hit("chat-team", "me", "My salary review is on Friday."),
								hit("chat-team", "me", "Ship it once the tests pass."))),
				new WorkTwinSuggest.RecentChat("Others", List.of(hit("chat-others", "priya", "Nothing of mine here."))));
		List<String> membersRead = new ArrayList<>();
		WorkTwinSuggest.Read<List<String>> members = chatId -> {
			membersRead.add(chatId);
			if ("chat-team".equals(chatId)) {
				return List.of("priya@test.com", "owner@test.com");
			}
			return List.of("dana@fabrikam.example");
		};

		// with no rules nobody's membership is read: what the owner wrote, of a length worth reading
		assertEquals(
				List.of("Send me the quote before lunch, please.", "My salary review is on Friday.",
						"Ship it once the tests pass."),
				WorkTwinPersonaInterview.ownTeamsMessages(chats, "me", gate(), members));
		assertEquals(List.of(), membersRead);
		assertEquals(List.of(), WorkTwinPersonaInterview.ownTeamsMessages(chats, null, gate(), members));

		// a never-ingest keyword drops the message, and someone kept out drops every chat they are in
		rule("never_keyword", "salary");
		assertEquals(List.of("Send me the quote before lunch, please.", "Ship it once the tests pass."),
				WorkTwinPersonaInterview.ownTeamsMessages(chats, "me", gate(), members));
		rule("never_sender", "dana@fabrikam.example");
		assertEquals(List.of("Ship it once the tests pass."),
				WorkTwinPersonaInterview.ownTeamsMessages(chats, "me", gate(), members));
		assertTrue(membersRead.containsAll(List.of("chat-dana", "chat-team")) && !membersRead.contains("chat-others"),
				membersRead.toString());

		// a chat whose people cannot be read, or one with someone who has no address, cannot be shown to be clear
		assertEquals(List.of(), WorkTwinPersonaInterview.ownTeamsMessages(chats, "me", gate(), chatId -> {
			throw new IllegalStateException("Graph is down");
		}));
		List<String> unnamed = new ArrayList<>();
		unnamed.add(null);
		assertEquals(List.of(), WorkTwinPersonaInterview.ownTeamsMessages(chats, "me", gate(), chatId -> unnamed));
	}

	// ---- helpers ----

	private Map<String, Object> call(Map<String, Object> answer, Boolean wrap, List<String> sources) {
		// the two long parts run in the call itself, where the server runs them as a job
		return WorkTwinPersonaInterview.step(user, answer, wrap, false, sources, model, places, false);
	}

	private Map<String, Object> back() {
		return WorkTwinPersonaInterview.step(user, null, null, true, null, model, places, false);
	}

	private void reachMirror() {
		call(null, null, List.of("none"));
		call(answer("dials", Map.of("values", Map.of())), null, null);
		call(answer("voice", Map.of("rounds", Map.of("r1", Map.of("pick", "b")))), null, null);
		call(answer("limits", Map.of()), null, null);
		call(null, true, null);
		assertEquals("mirror", step(call(answer("probe", Map.of()), null, null)).get("id"));
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> step(Map<String, Object> answer) {
		return (Map<String, Object>) answer.get("step");
	}

	private static Map<String, Object> answer(String stepId, Map<String, Object> answer) {
		Map<String, Object> given = new LinkedHashMap<>(answer);
		given.put("stepId", stepId);
		return given;
	}

	private static Map<String, Object> round(String from, String message, String a, String aIs, String b, String bIs) {
		return Map.of("from", from, "message", message, "a", a, "aIs", aIs, "b", b, "bIs", bIs);
	}

	private static Map<String, Object> hit(String chatId, String fromId, String text) {
		Map<String, Object> hit = new LinkedHashMap<>();
		hit.put("chatId", chatId);
		hit.put("fromId", fromId);
		hit.put("summary", text);
		return hit;
	}

	private WorkTwinSuggest.Gate gate() {
		return new WorkTwinSuggest.Gate(ownerId, ownerType, "owner@test.com");
	}

	private void rule(String kind, String value) throws Exception {
		try (PreparedStatement statement = connection
				.prepareStatement("INSERT INTO BRAIN_RULE (OWNER_ID, OWNER_TYPE, RULE_ID, KIND, "
						+ CollaborationDbUtils.column("VALUE") + ", CREATED_AT) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)")) {
			statement.setString(1, ownerId);
			statement.setString(2, ownerType);
			statement.setString(3, UUID.randomUUID().toString().substring(0, 30));
			statement.setString(4, kind);
			statement.setString(5, value);
			statement.executeUpdate();
		}
	}

	private int count(String table) throws Exception {
		try (Statement statement = connection.createStatement();
				ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM " + table)) {
			rs.next();
			return rs.getInt(1);
		}
	}

	private String text(String column) throws Exception {
		try (Statement statement = connection.createStatement();
				ResultSet rs = statement.executeQuery("SELECT " + column + " FROM BRAIN_PERSONA")) {
			rs.next();
			return rs.getString(1);
		}
	}
}
