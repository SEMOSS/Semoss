package prerna.reactor.collaboration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import prerna.auth.User;
import prerna.collaboration.BrainThreadMessages;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.io.connector.ms.teams.MicrosoftTeamsMessageHelper;
import prerna.om.Insight;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.execptions.SemossPixelException;

class WorkShowItemReactorUnitTests {

	private static final String CHAT = "19:abc123_def@thread.v2";

	@TempDir
	Path temp;

	// the room's working directory, one level down so a file beside it is outside the room
	private Path folder;
	private User user;
	private Insight insight;
	private MockedStatic<BrainThreadMessages> brain;
	private MockedStatic<MicrosoftLoginUtils> login;
	private MockedStatic<MicrosoftTeamsMessageHelper> teams;

	@BeforeEach
	void setup() throws Exception {
		folder = Files.createDirectories(temp.resolve("room"));
		user = mock(User.class);
		insight = mock(Insight.class);
		when(insight.getInsightFolder()).thenReturn(folder.toString());
		brain = mockStatic(BrainThreadMessages.class);
		login = mockStatic(MicrosoftLoginUtils.class);
		login.when(() -> MicrosoftLoginUtils.getValidAccessToken(user)).thenReturn("token");
		teams = mockStatic(MicrosoftTeamsMessageHelper.class);
	}

	@AfterEach
	void cleanup() {
		teams.close();
		login.close();
		brain.close();
	}

	private Map<?, ?> show(Map<String, String> arguments) {
		return (Map<?, ?>) reactor(arguments).execute().getValue();
	}

	private WorkShowItemReactor reactor(Map<String, String> arguments) {
		WorkShowItemReactor reactor = new WorkShowItemReactor() {
			@Override
			protected User getUser() {
				return user;
			}
		};
		reactor.setInsight(insight);
		NounStore store = new NounStore("show");
		arguments.forEach((key, value) -> store.makeNoun(key).addLiteral(value));
		reactor.setNounStore(store);
		return reactor;
	}

	private static void assertNothingShown(Runnable call, String expected) {
		RuntimeException error = assertThrows(RuntimeException.class, call::run);
		assertTrue(error.getMessage().contains(expected), error.getMessage());
		assertTrue(error.getMessage().endsWith("Nothing was shown."), error.getMessage());
	}

	@Test
	void anEmailRefOpensItsThreadAtTheStoredEmail() {
		brain.when(() -> BrainThreadMessages.composeSource(user, "email:k1"))
				.thenReturn(Map.of("id", "AAMk-long-graph-id", "subject", "Q3 budget", "threadId", "th-1"));
		Map<?, ?> shown = show(Map.of("emailRef", " email:k1 ", "threadId", "th-1"));
		assertEquals(true, shown.get("shown"));
		assertEquals("thread", shown.get("kind"));
		assertEquals("th-1", shown.get("threadId"));
		assertEquals("email", shown.get("source"));
		assertEquals("AAMk-long-graph-id", shown.get("messageId"));
		assertEquals("Q3 budget", shown.get("title"));
		assertTrue(String.valueOf(shown.get("note")).contains("Nothing was changed"));
	}

	@Test
	void anUnavailableEmailOrAnotherThreadsEmailIsNotShown() {
		brain.when(() -> BrainThreadMessages.composeSource(user, "email:gone")).thenReturn(null);
		assertNothingShown(() -> show(Map.of("emailRef", "email:gone")), "not available");
		brain.when(() -> BrainThreadMessages.composeSource(user, "email:k1"))
				.thenReturn(Map.of("id", "g1", "subject", "Q3", "threadId", "th-1"));
		assertNothingShown(() -> show(Map.of("emailRef", "email:k1", "threadId", "th-2")), "not on thread th-2");
	}

	@Test
	void aThreadOpensWhenItHasSomethingTheOwnerCanRead() {
		brain.when(() -> BrainThreadMessages.read(user, "th-teams", 20)).thenReturn(Map.of("source", "teams",
				"messages", List.of(Map.of("excluded", false, "subject", ""), Map.of("excluded", true))));
		Map<?, ?> shown = show(Map.of("threadId", "th-teams"));
		assertEquals("thread", shown.get("kind"));
		assertEquals("th-teams", shown.get("threadId"));
		assertEquals("teams", shown.get("source"));
		assertEquals("", shown.get("title"));
		brain.when(() -> BrainThreadMessages.read(user, "th-mail", 20)).thenReturn(Map.of("source", "email",
				"messages", List.of(Map.of("excluded", false, "subject", "Plan"),
						Map.of("excluded", false, "subject", "RE: Plan"))));
		assertEquals("RE: Plan", show(Map.of("threadId", "th-mail")).get("title"));
	}

	@Test
	void missingCalendarAndHiddenThreadsAreNotShown() {
		brain.when(() -> BrainThreadMessages.read(user, "th-x", 20))
				.thenThrow(new IllegalArgumentException("Thread not found"));
		assertNothingShown(() -> show(Map.of("threadId", "th-x")), "Copy threadId exactly");
		brain.when(() -> BrainThreadMessages.read(user, "th-meeting", 20))
				.thenReturn(Map.of("source", "calendar", "messages", List.of(Map.of("excluded", false))));
		assertNothingShown(() -> show(Map.of("threadId", "th-meeting")), "not a calendar thread");
		brain.when(() -> BrainThreadMessages.read(user, "th-hidden", 20))
				.thenReturn(Map.of("source", "email", "messages", List.of(Map.of("excluded", true))));
		assertNothingShown(() -> show(Map.of("threadId", "th-hidden")), "nothing the owner can read");
	}

	@Test
	void aTeamsChatOpensWithItsCanonicalIdTitleAndLink() throws Exception {
		teams.when(() -> MicrosoftTeamsMessageHelper.getChat("token", CHAT, null)).thenReturn(Map.of("id", CHAT,
				"displayName", "Dana Lee, Raj Patel", "webUrl", "https://teams.microsoft.com/l/chat/1"));
		teams.when(() -> MicrosoftTeamsMessageHelper.listChatMessages("token", CHAT, 1, 1)).thenReturn(List.of());
		Map<?, ?> shown = show(Map.of("chatId", CHAT));
		assertEquals("chat", shown.get("kind"));
		assertEquals(CHAT, shown.get("chatId"));
		assertEquals("Dana Lee, Raj Patel", shown.get("title"));
		assertEquals("https://teams.microsoft.com/l/chat/1", shown.get("webLink"));
	}

	@ParameterizedTest
	@ValueSource(strings = { "19:abc/../me", "19:abc?$top=1", "19:abc def" })
	void aChatIdThatCouldChangeTheGraphUrlIsRefusedBeforeAnyCall(String chatId) {
		assertNothingShown(() -> show(Map.of("chatId", chatId)), "not a Teams chat id");
		login.verifyNoInteractions();
		teams.verifyNoInteractions();
	}

	@Test
	void aMissingChatFailsAndALoginErrorKeepsItsOwnException() {
		teams.when(() -> MicrosoftTeamsMessageHelper.getChat(anyString(), anyString(), any()))
				.thenThrow(new IllegalArgumentException("GET request returned HTTP 404"));
		assertNothingShown(() -> show(Map.of("chatId", CHAT)), "not found, or the owner cannot read it");
		SemossPixelException loginError = new SemossPixelException("Please login to your Microsoft account", false);
		login.when(() -> MicrosoftLoginUtils.getValidAccessToken(user)).thenThrow(loginError);
		assertSame(loginError, assertThrows(SemossPixelException.class, () -> show(Map.of("chatId", CHAT))));
		teams.verify(() -> MicrosoftTeamsMessageHelper.listChatMessages(anyString(), anyString(), anyInt(), anyInt()),
				org.mockito.Mockito.never());
	}

	@Test
	void aRoomFileOpensByItsPathInsideTheRoom() throws Exception {
		Files.createDirectories(folder.resolve("docs"));
		Files.writeString(folder.resolve("docs/report.pdf"), "pdf");
		for (String path : List.of("docs/report.pdf", "/docs/report.pdf", "docs/../docs/report.pdf")) {
			Map<?, ?> shown = show(Map.of("path", path));
			assertEquals("file", shown.get("kind"));
			assertEquals("/docs/report.pdf", shown.get("path"));
			assertEquals("report.pdf", shown.get("name"));
		}
	}

	@Test
	void aMissingOutsideOrFolderPathIsNotShown() throws Exception {
		Files.createDirectories(folder.resolve("docs"));
		Files.writeString(temp.resolve("outside.txt"), "x");
		for (String path : List.of("missing.pdf", "../outside.txt", "docs")) {
			assertNothingShown(() -> show(Map.of("path", path)), "Download it first");
		}
	}

	@Test
	void exactlyOneItemIsShownPerCall() {
		for (Map<String, String> arguments : List.of(Map.<String, String>of(), Map.of("threadId", " "),
				Map.of("threadId", "th-1", "path", "a.pdf"), Map.of("emailRef", "email:k1", "chatId", CHAT))) {
			assertNothingShown(() -> show(arguments), "Pass one item");
		}
		brain.verifyNoInteractions();
		login.verifyNoInteractions();
		teams.verifyNoInteractions();
	}
}
