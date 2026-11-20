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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.javatuples.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.collaboration.WorkTwinUpdates.Copy;
import prerna.collaboration.WorkTwinUpdates.Finding;
import prerna.collaboration.WorkTwinUpdates.Source;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.io.connector.ms.onedrive.MicrosoftOneDriveHelper;
import prerna.io.connector.ms.teams.MicrosoftTeamsMessageHelper;
import prerna.om.ThreadStore;
import prerna.project.api.IProject;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.EngineUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

/**
 * Digital Twin update suggestions: what a twin's record gives a check to compare, what the check concludes of each
 * copy from what the owner's Microsoft 365 answers, and what its rows keep through a dismissal and an Update.
 * Microsoft 365 is a fake; the rows are in a real schema.
 */
class WorkTwinUpdatesUnitTests {

	private static final Instant COPIED = Instant.parse("2026-11-12T17:00:00Z");
	private static final String BEFORE = "2026-11-10T09:00:00Z";
	private static final String AFTER = "2026-11-18T15:00:00Z";
	private static final String LATER = "2026-11-19T08:30:00Z";
	private static final String TWIN = "twin-one";
	private static final String THREAD = "public/emails/Email thread - Budget.md";

	private Connection connection;
	private MockedStatic<SystemEngineRegistry> registry;
	private User user;
	private String ownerId;
	private String ownerType;
	private BrainMailFolders.Tree folders;
	// what the owner's Microsoft 365 holds, and how often each place was read
	private final List<Map<String, Object>> mail = new ArrayList<>();
	private final Map<String, Object> chats = new HashMap<>();
	private final Map<String, Object> drive = new HashMap<>();
	private final List<String> read = new ArrayList<>();
	private int unlisted;
	private final Source source = new Source() {
		@Override
		public List<Map<String, Object>> threads(Collection<String> conversationIds, AtomicInteger unread) {
			read.add("mail");
			unread.set(unlisted);
			return mail;
		}

		@Override
		@SuppressWarnings("unchecked")
		public List<Map<String, Object>> chat(String chatId) throws Exception {
			read.add("chat " + chatId);
			if (chats.get(chatId) instanceof Exception failure) {
				throw failure;
			}
			return (List<Map<String, Object>>) chats.get(chatId);
		}

		@Override
		@SuppressWarnings("unchecked")
		public Map<String, Object> file(String driveId, String itemId) throws Exception {
			read.add("file " + itemId);
			if (drive.get(itemId) instanceof Exception failure) {
				throw failure;
			}
			return (Map<String, Object>) drive.get(itemId);
		}
	};

	@BeforeEach
	void setUp() throws Exception {
		connection = DriverManager.getConnection("jdbc:h2:mem:twin_updates_" + UUID.randomUUID());
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
		// a new owner per test: jobs and locks are per owner on a server
		token.setId("owner-" + UUID.randomUUID());
		token.setName("Owner");
		token.setProvider(AuthProvider.NATIVE);
		user = new User();
		user.setAccessToken(token);
		Pair<String, String> owner = User.getPrimaryUserIdAndTypePair(user);
		ownerId = owner.getValue0();
		ownerType = owner.getValue1();
		folders = BrainMailFolders.build(List.of(Map.of("id", "f-inbox", "displayName", "Inbox", "wellKnownName",
				"inbox"), Map.of("id", "f-deleted", "displayName", "Deleted Items", "wellKnownName", "deleteditems"),
				Map.of("id", "f-deleted-old", "displayName", "Old", "parentFolderId", "f-deleted"),
				Map.of("id", "f-junk", "displayName", "Junk Email", "wellKnownName", "junkemail")), null);
	}

	@AfterEach
	void tearDown() throws Exception {
		registry.close();
		connection.close();
	}

	// ---- what a twin's record gives a check ----

	@Test
	void aTwinsRecordGivesTheCopiesACheckCanCompare(@TempDir Path assets) throws Exception {
		Instant written = Instant.parse("2026-11-12T17:00:00Z");
		for (String path : List.of(THREAD, "public/teams-chats/Chat - Hiring.md", "public/files/Plan.docx",
				"public/files/Teams deck.pptx", "public/emails/report.pdf", "public/emails/Email - Lone.md",
				"public/meetings/Sync.md", "secret.txt")) {
			Path file = assets.resolve(path);
			Files.createDirectories(file.getParent());
			Files.writeString(file, "copy");
			Files.setLastModifiedTime(file, FileTime.from(written));
		}
		Map<String, Object> record = new LinkedHashMap<>();
		record.put(THREAD, "conversation:conv-1");
		record.put("public/teams-chats/Chat - Hiring.md", "19:abc@thread.v2");
		record.put("public/files/Plan.docx", "item-1");
		record.put("public/files/Teams deck.pptx", "item-2");
		record.put("drive:item-2", "b!drive");
		// not compared: an attachment, a single email, a meeting, a copy that is gone, and paths that do not name one
		// file among the twin's own
		record.put("public/emails/report.pdf", "AAMk=:att-1");
		record.put("public/emails/Email - Lone.md", "message:AAMk=");
		record.put("public/meetings/Sync.md", "event-1");
		record.put("public/files/Gone.docx", "item-3");
		record.put("public/files/../../secret.txt", "item-4");
		record.put("public/files/..", "item-6");
		record.put("public/files/deep/Plan.docx", "item-5");
		record.put("public/files/Odd.docx", 7);
		Files.writeString(assets.resolve(WorkTwinUpdates.RECORD), CollaborationDbUtils.toJson(record));

		assertEquals(List.of(new Copy(TWIN, THREAD, "email", "conversation:conv-1", null, written),
				new Copy(TWIN, "public/teams-chats/Chat - Hiring.md", "chat", "19:abc@thread.v2", null, written),
				new Copy(TWIN, "public/files/Plan.docx", "file", "item-1", null, written),
				new Copy(TWIN, "public/files/Teams deck.pptx", "file", "item-2", "b!drive", written)),
				WorkTwinUpdates.copies(TWIN, assets));
	}

	@Test
	void aTwinWithNoRecordHoldsNothingToCheckAndOneWithAnUnreadableRecordIsLeftAlone(@TempDir Path assets)
			throws Exception {
		assertEquals(List.of(), WorkTwinUpdates.copies(TWIN, assets));
		Files.writeString(assets.resolve(WorkTwinUpdates.RECORD), "{ not json");
		assertNull(WorkTwinUpdates.copies(TWIN, assets));
	}

	// ---- what a check concludes ----

	@Test
	void aThreadIsChangedByMailThatArrivedAfterTheCopyAndIsPartOfTheThread() {
		mail.add(email("conv-1", BEFORE, "Sam Park", "f-inbox", false));
		mail.add(email("conv-1", AFTER, "Dana Lee", "f-inbox", false));
		// an unsent reply and a deleted email are no part of what a copy holds
		mail.add(email("conv-1", LATER, "Owner", "f-inbox", true));
		mail.add(email("conv-1", LATER, "Sam Park", "f-deleted", false));

		Finding finding = only(thread("conv-1"));
		assertEquals("changed", finding.state());
		assertEquals(Instant.parse(AFTER), finding.sourceAt());
		assertEquals(Map.of("count", 1, "latestFrom", "Dana Lee", "title", "Budget"), finding.detail());
	}

	@Test
	void aThreadIsCurrentOrGoneOnlyWhenTheMailboxListedAllOfIt() {
		mail.add(email("conv-1", BEFORE, "Sam Park", "f-inbox", false));
		mail.add(email("conv-2", BEFORE, "Sam Park", "f-deleted", false));
		Copy kept = thread("conv-1");
		Copy deleted = new Copy(TWIN, "public/emails/Email thread - Old.md", "email", "conversation:conv-2", null,
				COPIED);
		assertEquals(List.of("current", "gone"), states(kept, deleted));

		// a listing that failed could be either thread's
		unlisted = 1;
		assertEquals(List.of("unknown", "unknown"), states(kept, deleted));

		// a thread at its limit may hold a newer email that was not listed
		unlisted = 0;
		for (int i = 1; i < WorkTwinUpdates.PER_THREAD; i++) {
			mail.add(email("conv-1", BEFORE, "Sam Park", "f-inbox", false));
		}
		assertEquals(List.of("unknown", "gone"), states(kept, deleted));
	}

	@Test
	void aThreadIsGoneWhenItWasDeletedAndNotWhenItSitsInAFolderThatIsNotRead() {
		// under Deleted Items is deleted too
		mail.add(email("conv-1", BEFORE, "Sam Park", "f-deleted-old", false));
		// Junk Email is not read, but what is in it is still in the mailbox: its copy is not one to remove, and
		// newer mail there is none the import would bring in
		mail.add(email("conv-2", BEFORE, "Sam Park", "f-junk", false));
		mail.add(email("conv-2", AFTER, "Sam Park", "f-junk", false));
		Copy junk = new Copy(TWIN, "public/emails/Email thread - Offer.md", "email", "conversation:conv-2", null,
				COPIED);
		// nothing is listed of a thread that is in the mailbox no more
		Copy purged = new Copy(TWIN, "public/emails/Email thread - Purged.md", "email", "conversation:conv-3", null,
				COPIED);

		assertEquals(List.of("gone", "current", "gone"), states(thread("conv-1"), junk, purged));
		assertTrue(folders.skipped("f-junk") && folders.skipped("f-deleted") && folders.skipped("f-deleted-old"));
		assertTrue(folders.deleted("f-deleted") && folders.deleted("f-deleted-old"));
		assertFalse(folders.deleted("f-junk") || folders.deleted("f-inbox") || folders.skipped("f-inbox"));
	}

	@Test
	void aChatIsChangedByWhatPeopleWroteAfterTheCopy() {
		Copy quiet = chat("19:quiet@thread.v2");
		Copy busy = chat("19:busy@thread.v2");
		Copy left = chat("19:left@thread.v2");
		Copy failing = chat("19:failing@thread.v2");
		chats.put("19:quiet@thread.v2", List.of(message(BEFORE, "Priya", "message", false),
				// someone joining, and a message that was taken back, are not something written
				message(AFTER, null, "systemEventMessage", false), message(AFTER, "Priya", "message", true)));
		chats.put("19:busy@thread.v2",
				List.of(message(LATER, "Priya", "message", false), message(AFTER, "Sam", "message", false)));
		chats.put("19:failing@thread.v2", new IllegalStateException("Graph returned HTTP 503"));

		List<Finding> findings = WorkTwinUpdates.compare(List.of(quiet, busy, left, failing), source, folders, null);
		assertEquals(List.of("current", "changed", "gone", "unknown"), findings.stream().map(Finding::state).toList());
		assertEquals(Instant.parse(LATER), findings.get(1).sourceAt());
		assertEquals(Map.of("count", 2, "latestFrom", "Priya"), findings.get(1).detail());
	}

	@Test
	void aChatsCountIsAFloorWhenEverythingReadIsNewerThanTheCopy() {
		Copy busy = chat("19:busy@thread.v2");
		Copy whole = chat("19:whol@thread.v2");
		Copy events = chat("19:evnt@thread.v2");
		// the newest fifty entries, every one after the copy: forty-nine messages and someone joining
		List<Map<String, Object>> newest = new ArrayList<>();
		for (int i = 1; i < WorkTwinUpdates.CHAT_MESSAGES; i++) {
			newest.add(message(AFTER, "Sam", "message", false));
		}
		chats.put("19:busy@thread.v2", with(newest, message(LATER, null, "systemEventMessage", false)));
		// the same, but the fiftieth is from before the copy: the read went back as far as the copy
		chats.put("19:whol@thread.v2", with(newest, message(BEFORE, "Sam", "message", false)));
		// fifty entries after the copy and none of them a message: what people wrote may lie further back
		List<Map<String, Object>> joins = new ArrayList<>();
		for (int i = 0; i < WorkTwinUpdates.CHAT_MESSAGES; i++) {
			joins.add(message(AFTER, null, "systemEventMessage", false));
		}
		chats.put("19:evnt@thread.v2", joins);

		List<Finding> findings = WorkTwinUpdates.compare(List.of(busy, whole, events), source, folders, null);
		assertEquals(List.of("changed", "changed", "unknown"), findings.stream().map(Finding::state).toList());
		assertEquals(Map.of("count", 49, "more", true, "latestFrom", "Sam"), findings.get(0).detail());
		assertEquals(Map.of("count", 49, "latestFrom", "Sam"), findings.get(1).detail());
	}

	@Test
	void theChatsAfterOneGraphDidNotAnswerForAreNotAsked() {
		chats.put("19:dead@thread.v2", new IllegalStateException("Failed to execute GET request: Read timed out"));
		chats.put("19:next@thread.v2", List.of(message(AFTER, "Sam", "message", false)));

		assertEquals(List.of("unknown", "unknown"), states(chat("19:dead@thread.v2"), chat("19:next@thread.v2")));
		assertEquals(List.of("chat 19:dead@thread.v2"), read);
	}

	@Test
	void aFileIsChangedByALaterModifiedTimeAndGoneOnlyWhereItsDriveIsKnown() {
		Copy same = file("public/files/Same.docx", "item-same", "b!drive");
		Copy edited = file("public/files/Plan.docx", "item-edited", "b!drive");
		Copy deleted = file("public/files/Old.xlsx", "item-deleted", "b!drive");
		// an id the record names no drive for may live in a drive that was not asked
		Copy elsewhere = file("public/files/Deck.pptx", "item-elsewhere", null);
		drive.put("item-same", Map.of("name", "Same.docx", "lastModifiedDateTime", BEFORE));
		drive.put("item-edited", Map.of("name", "Q4 plan.docx", "lastModifiedDateTime", AFTER, "lastModifiedBy",
				"Sam Park", "webUrl", "https://contoso.sharepoint.com/plan"));

		List<Finding> findings = WorkTwinUpdates.compare(List.of(same, edited, deleted, elsewhere), source, folders,
				null);
		assertEquals(List.of("current", "changed", "gone", "unknown"), findings.stream().map(Finding::state).toList());
		assertEquals(Instant.parse(AFTER), findings.get(1).sourceAt());
		assertEquals(Map.of("latestFrom", "Sam Park", "title", "Q4 plan.docx", "webUrl",
				"https://contoso.sharepoint.com/plan"), findings.get(1).detail());
	}

	@Test
	void theFilesAfterOneGraphDidNotAnswerForAreNotAsked() {
		drive.put("item-1", new IllegalStateException("Read timed out"));
		drive.put("item-2", Map.of("lastModifiedDateTime", AFTER));

		assertEquals(List.of("unknown", "unknown"), states(file("public/files/A.docx", "item-1", null),
				file("public/files/B.docx", "item-2", null)));
		assertEquals(List.of("file item-1"), read);
	}

	@Test
	void theOlderOfTwoCopiesOfOneItemIsADuplicateAndEachSourceIsReadOnce() {
		mail.add(email("conv-1", AFTER, "Dana Lee", "f-inbox", false));
		Copy older = thread("conv-1");
		Copy newer = new Copy(TWIN, "public/emails/Email thread - Budget (2).md", "email", "conversation:conv-1",
				null, COPIED.plus(Duration.ofDays(1)));
		// the same thread in another twin is that twin's own copy
		Copy other = new Copy("twin-two", THREAD, "email", "conversation:conv-1", null, COPIED);

		List<Finding> findings = WorkTwinUpdates.compare(List.of(older, newer, other), source, folders, null);
		Map<Copy, Finding> byCopy = new HashMap<>();
		findings.forEach(finding -> byCopy.put(finding.copy(), finding));
		assertEquals("duplicate", byCopy.get(older).state());
		assertEquals(Map.of("newerCopy", "public/emails/Email thread - Budget (2).md"), byCopy.get(older).detail());
		assertEquals("changed", byCopy.get(newer).state());
		assertEquals("changed", byCopy.get(other).state());
		assertEquals(List.of("mail"), read);
	}

	// ---- what the rows keep ----

	@Test
	void aSuggestionWaitsUntilItIsDismissedAndComesBackWhenTheSourceMovesAgain() {
		Copy copy = thread("conv-1");
		Finding changed = new Finding(copy, "changed", Instant.parse(AFTER),
				Map.of("count", 2, "latestFrom", "Dana Lee", "title", "Budget"));
		WorkTwinUpdates.save(ownerId, ownerType, Set.of(TWIN), List.of(changed));

		Map<String, Object> waiting = only(WorkTwinUpdates.open(ownerId, ownerType));
		assertEquals(TWIN, waiting.get("twinId"));
		assertEquals("changed", waiting.get("change"));
		assertEquals(THREAD, waiting.get("path"));
		assertEquals("email", waiting.get("kind"));
		assertEquals("conversation:conv-1", waiting.get("key"));
		assertEquals(2, waiting.get("count"));
		assertEquals("Dana Lee", waiting.get("latestFrom"));
		assertEquals(AFTER, waiting.get("latestAt"));
		assertEquals("2026-11-12T17:00:00Z", waiting.get("copiedAt"));

		String id = (String) waiting.get("id");
		assertTrue(WorkTwinUpdates.settle(user, id, "dismissed"));
		assertEquals(List.of(), WorkTwinUpdates.open(ownerId, ownerType));
		// the same change found again stays dismissed
		WorkTwinUpdates.save(ownerId, ownerType, Set.of(TWIN), List.of(changed));
		assertEquals(List.of(), WorkTwinUpdates.open(ownerId, ownerType));
		// newer mail brings it back
		WorkTwinUpdates.save(ownerId, ownerType, Set.of(TWIN),
				List.of(new Finding(copy, "changed", Instant.parse(LATER), Map.of("count", 3))));
		assertEquals(LATER, only(WorkTwinUpdates.open(ownerId, ownerType)).get("latestAt"));

		assertThrows(IllegalArgumentException.class, () -> WorkTwinUpdates.settle(user, id, "removed"));
		assertFalse(WorkTwinUpdates.settle(user, "no-such-row", "dismissed"));
	}

	@Test
	void anUpdateMakesTheCopyCurrentAsOfNowEvenForACheckThatWasAlreadyRunning() {
		Copy copy = thread("conv-1");
		Finding changed = new Finding(copy, "changed", Instant.parse(AFTER), Map.of("count", 1));
		WorkTwinUpdates.save(ownerId, ownerType, Set.of(TWIN), List.of(changed));
		String id = (String) only(WorkTwinUpdates.open(ownerId, ownerType)).get("id");

		assertTrue(WorkTwinUpdates.settle(user, id, "applied"));
		assertEquals(List.of(), WorkTwinUpdates.open(ownerId, ownerType));
		// the copy is now known current as of the Update, whatever its file says
		Instant known = only(WorkTwinUpdates.known(ownerId, ownerType, List.of(copy))).copiedAt();
		assertTrue(known.isAfter(Instant.parse(LATER)));
		// a check that read the copy before the Update found the change it brought in
		WorkTwinUpdates.save(ownerId, ownerType, Set.of(TWIN), List.of(changed));
		assertEquals(List.of(), WorkTwinUpdates.open(ownerId, ownerType));
		// what changes after it is found
		Instant next = known.plus(Duration.ofHours(1));
		WorkTwinUpdates.save(ownerId, ownerType, Set.of(TWIN),
				List.of(new Finding(copy, "changed", next, Map.of("count", 1))));
		assertEquals(1, WorkTwinUpdates.open(ownerId, ownerType).size());
	}

	@Test
	void aCopyFoundCurrentLosesItsSuggestionAndOneThatCouldNotBeReadKeepsIt() {
		Copy copy = file("public/files/Old.xlsx", "item-1", "b!drive");
		WorkTwinUpdates.save(ownerId, ownerType, Set.of(TWIN), List.of(Finding.of(copy, "gone")));
		assertEquals("gone", only(WorkTwinUpdates.open(ownerId, ownerType)).get("change"));

		WorkTwinUpdates.save(ownerId, ownerType, Set.of(TWIN), List.of(Finding.of(copy, "unknown")));
		assertEquals(1, WorkTwinUpdates.open(ownerId, ownerType).size());
		// the original is back
		WorkTwinUpdates.save(ownerId, ownerType, Set.of(TWIN), List.of(Finding.of(copy, "current")));
		assertEquals(List.of(), WorkTwinUpdates.open(ownerId, ownerType));
	}

	@Test
	void theRowsOfCopiesATwinNoLongerHoldsAreDroppedOnlyForTheTwinsThatWereRead() {
		Copy here = thread("conv-1");
		Copy there = new Copy("twin-two", THREAD, "email", "conversation:conv-9", null, COPIED);
		WorkTwinUpdates.save(ownerId, ownerType, Set.of(TWIN, "twin-two"),
				List.of(Finding.of(here, "gone"), Finding.of(there, "gone")));
		assertEquals(2, WorkTwinUpdates.open(ownerId, ownerType).size());

		// the first twin was read and holds nothing now; the second could not be read
		WorkTwinUpdates.save(ownerId, ownerType, Set.of(TWIN), List.of());
		assertEquals("twin-two", only(WorkTwinUpdates.open(ownerId, ownerType)).get("twinId"));
	}

	@Test
	void aPathThatComesToNameAnotherItemStartsOverFromItsFilesTime() {
		Copy first = thread("conv-1");
		WorkTwinUpdates.save(ownerId, ownerType, Set.of(TWIN), List.of(Finding.of(first, "gone")));
		String id = (String) only(WorkTwinUpdates.open(ownerId, ownerType)).get("id");
		WorkTwinUpdates.settle(user, id, "applied");

		// another thread saved under the name the first one had
		Instant written = COPIED.plus(Duration.ofDays(3));
		Copy second = new Copy(TWIN, THREAD, "email", "conversation:conv-2", null, written);
		assertEquals(written, only(WorkTwinUpdates.known(ownerId, ownerType, List.of(second))).copiedAt());
		WorkTwinUpdates.save(ownerId, ownerType, Set.of(TWIN), List.of(Finding.of(second, "current")));
		assertEquals(written, only(WorkTwinUpdates.known(ownerId, ownerType, List.of(second))).copiedAt());
		assertEquals(List.of(), WorkTwinUpdates.open(ownerId, ownerType));
	}

	// ---- when a check runs ----

	@Test
	void aCheckIsStartedOnlyWhenOneIsDue() throws Exception {
		// the job itself is not under test: it would run on a thread that cannot see this database
		try (MockedStatic<MicrosoftLoginUtils> login = mockStatic(MicrosoftLoginUtils.class);
				MockedStatic<CollaborationJobUtils> jobs = mockStatic(CollaborationJobUtils.class)) {
			login.when(() -> MicrosoftLoginUtils.getValidAccessToken(any())).thenReturn("token");

			// never checked: due
			assertTrue(WorkTwinUpdates.watch(user, ownerId, ownerType));
			// checked two hours ago, and the default is once a day
			jobs.when(() -> CollaborationJobUtils.latest(ownerId, ownerType, WorkTwinUpdates.KIND))
					.thenReturn(Map.of("startedAt", Instant.now().minus(Duration.ofHours(2)).toString()));
			assertFalse(WorkTwinUpdates.watch(user, ownerId, ownerType));
			// the owner asked for every hour
			try (MockedStatic<BrainThreadClassifier> classifier = mockStatic(BrainThreadClassifier.class)) {
				assertEquals(24, BrainProfileUtils.getSettings(ownerId, ownerType).get("twinCheckEveryHours"));
				assertEquals(1,
						BrainProfileUtils.saveSettings(user, Map.of("twinCheckEveryHours", 1)).get("twinCheckEveryHours"));
				assertThrows(IllegalArgumentException.class,
						() -> BrainProfileUtils.saveSettings(user, Map.of("twinCheckEveryHours", 2)));
				assertTrue(WorkTwinUpdates.watch(user, ownerId, ownerType));
				// and then for never
				BrainProfileUtils.saveSettings(user, Map.of("twinCheckEveryHours", 0));
				assertFalse(WorkTwinUpdates.watch(user, ownerId, ownerType));
				BrainProfileUtils.saveSettings(user, Map.of("twinCheckEveryHours", 1));
			}
			// not signed in to Microsoft 365: nothing to check with, and no job is recorded
			login.when(() -> MicrosoftLoginUtils.getValidAccessToken(any()))
					.thenThrow(new IllegalStateException("Sign in to Microsoft"));
			assertFalse(WorkTwinUpdates.watch(user, ownerId, ownerType));
			jobs.verify(() -> CollaborationJobUtils.start(eq(ownerId), eq(ownerType), eq(WorkTwinUpdates.KIND),
					anyMap(), any()), times(2));
		}
	}

	// ---- a whole check, as its job runs it ----

	@Test
	void aCheckFlagsATwinsCopiedChatThatPeopleWroteInSince(@TempDir Path assets, @TempDir Path empty)
			throws Exception {
		String chat = "public/teams-chats/Chat - Hiring.md";
		holds(assets, Map.of(chat, "19:abc@thread.v2"));
		IProject twin = twin(TWIN, "Holiday");
		// another twin of theirs, which holds nothing a check compares
		IProject other = twin("twin-three", "Offsite");
		CollaborationJobUtils.Job job = job();
		try (MockedStatic<SecurityProjectUtils> projects = owns(Map.of("project_id", TWIN, "project_name", "Holiday"),
				Map.of("project_id", "twin-three", "project_name", "Offsite"));
				MockedStatic<Utility> utility = mockStatic(Utility.class);
				MockedStatic<EngineUtility> engines = mockStatic(EngineUtility.class);
				MockedStatic<MicrosoftLoginUtils> login = mockStatic(MicrosoftLoginUtils.class);
				MockedStatic<MicrosoftTeamsMessageHelper> teams = mockStatic(MicrosoftTeamsMessageHelper.class)) {
			utility.when(Utility::getCurrentSqlTimestampUTC).thenAnswer(call -> utcNow());
			utility.when(() -> Utility.getProject(TWIN)).thenReturn(twin);
			utility.when(() -> Utility.getProject("twin-three")).thenReturn(other);
			engines.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.PROJECT, TWIN, "Holiday"))
					.thenReturn(assets.toString());
			engines.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.PROJECT, "twin-three",
					"Offsite")).thenReturn(empty.toString());
			login.when(() -> MicrosoftLoginUtils.getValidAccessToken(user)).thenReturn("token");
			// read with a time limit, so a chat Graph never answers for cannot hold the job for good
			teams.when(() -> MicrosoftTeamsMessageHelper.listChatMessages("token", "19:abc@thread.v2", 1,
					WorkTwinUpdates.CHAT_MESSAGES, false, BrainGraphHeaderSource.TIMEOUT))
					.thenReturn(List.of(message(LATER, "Priya", "message", false),
							message(AFTER, "Sam", "message", false), message(BEFORE, "Sam", "message", false)));

			WorkTwinUpdates.check(user, job);
			// the sign-in is asked for before anything is read and again at the read, so a token that runs out
			// during a long check is renewed
			login.verify(() -> MicrosoftLoginUtils.getValidAccessToken(user), times(2));
		}
		Map<String, Object> waiting = only(WorkTwinUpdates.open(ownerId, ownerType));
		assertEquals(TWIN, waiting.get("twinId"));
		assertEquals(chat, waiting.get("path"));
		assertEquals("changed", waiting.get("change"));
		assertEquals(2, waiting.get("count"));
		assertEquals("Priya", waiting.get("latestFrom"));
		assertEquals(LATER, waiting.get("latestAt"));
		// what each twin's page says of the check: the one copy of the first was compared, and the other, which
		// was read too, holds none
		verify(job).count("byTwin", Map.of(TWIN, Map.of("copies", 1, "unknown", 0), "twin-three",
				Map.of("copies", 0, "unknown", 0)));
		// and the job's thread is left with nobody signed in, as it was
		assertNull(ThreadStore.getUser());
	}

	@Test
	void aTwinThatCannotBeOpenedFailsTheCheckOnceWhatTheOthersHoldIsKept(@TempDir Path assets) throws Exception {
		String file = "public/files/Old.xlsx";
		holds(assets, Map.of(file, "item-1", "drive:item-1", "b!drive", "public/teams-chats/Chat - Hiring.md",
				"19:failing@thread.v2"));
		IProject twin = twin(TWIN, "Holiday");
		CollaborationJobUtils.Job job = job();
		try (MockedStatic<SecurityProjectUtils> projects = owns(Map.of("project_id", TWIN, "project_name", "Holiday"),
				// named on its pages by its display name
				Map.of("project_id", "twin-two", "project_name", "ryans", "project_display_name", "Ryans Digital Twin"));
				MockedStatic<Utility> utility = mockStatic(Utility.class);
				MockedStatic<EngineUtility> engines = mockStatic(EngineUtility.class);
				MockedStatic<MicrosoftLoginUtils> login = mockStatic(MicrosoftLoginUtils.class);
				MockedStatic<MicrosoftTeamsMessageHelper> teams = mockStatic(MicrosoftTeamsMessageHelper.class);
				MockedStatic<MicrosoftOneDriveHelper> drives = mockStatic(MicrosoftOneDriveHelper.class)) {
			// the drive holds no such file any more: its original is gone
			drives.when(() -> MicrosoftOneDriveHelper.fileState("token", "b!drive", "item-1", null)).thenReturn(null);
			utility.when(Utility::getCurrentSqlTimestampUTC).thenAnswer(call -> utcNow());
			utility.when(() -> Utility.getProject(TWIN)).thenReturn(twin);
			utility.when(() -> Utility.getProject("twin-two"))
					.thenThrow(new SemossPixelException("Project twin-two could not be loaded"));
			engines.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.PROJECT, TWIN, "Holiday"))
					.thenReturn(assets.toString());
			login.when(() -> MicrosoftLoginUtils.getValidAccessToken(user)).thenReturn("token");
			teams.when(() -> MicrosoftTeamsMessageHelper.listChatMessages("token", "19:failing@thread.v2", 1,
					WorkTwinUpdates.CHAT_MESSAGES, false, BrainGraphHeaderSource.TIMEOUT))
					.thenThrow(new IllegalStateException("Graph returned HTTP 503"));

			IllegalStateException failure = assertThrows(IllegalStateException.class,
					() -> WorkTwinUpdates.check(user, job));
			// the owner is told which twin, by the name its pages show
			assertTrue(failure.getMessage().contains("Ryans Digital Twin"), failure.getMessage());
			assertFalse(failure.getMessage().contains("Holiday"), failure.getMessage());
		}
		// what the twin that opened holds was compared and kept all the same
		Map<String, Object> waiting = only(WorkTwinUpdates.open(ownerId, ownerType));
		assertEquals(file, waiting.get("path"));
		assertEquals("gone", waiting.get("change"));
		// its page says one of its two copies could not be compared; the twin that did not open is not in here
		verify(job).count("byTwin", Map.of(TWIN, Map.of("copies", 2, "unknown", 1)));
		assertNull(ThreadStore.getUser());
	}

	// ---- a copy the owner's page just made ----

	@Test
	void aCopyIsNotedWhenItIsMadeAndReplacesWhatWasKnownOfTheCopyThatHadItsName(@TempDir Path assets)
			throws Exception {
		String chat = "public/teams-chats/Chat - Hiring.md";
		holds(assets, Map.of(chat, "19:abc@thread.v2", "public/files/Notes.txt", "AAMk=:att-1"));
		Copy first = new Copy(TWIN, chat, "chat", "19:abc@thread.v2", null, COPIED);
		// the copy that had this name was a week older, and an update was waiting for it
		Instant before = COPIED.minus(Duration.ofDays(7));
		Copy old = new Copy(TWIN, chat, "chat", "19:abc@thread.v2", null, before);
		WorkTwinUpdates.save(ownerId, ownerType, Set.of(TWIN),
				List.of(new Finding(old, "changed", Instant.parse(BEFORE), Map.of("count", 4))));
		assertEquals(1, WorkTwinUpdates.open(ownerId, ownerType).size());
		assertEquals(before, only(WorkTwinUpdates.known(ownerId, ownerType, List.of(first))).copiedAt());

		IProject twin = twin(TWIN, "Holiday");
		User onThread = ThreadStore.getUser();
		ThreadStore.setUser(user);
		try (MockedStatic<SecurityProjectUtils> projects = owns(Map.of("project_id", TWIN, "project_name", "Holiday"));
				MockedStatic<Utility> utility = mockStatic(Utility.class);
				MockedStatic<EngineUtility> engines = mockStatic(EngineUtility.class)) {
			utility.when(() -> Utility.getProject(TWIN)).thenReturn(twin);
			engines.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.PROJECT, TWIN, "Holiday"))
					.thenReturn(assets.toString());

			assertTrue(WorkTwinUpdates.copied(user, TWIN, chat));
			// an attachment is no copy a check compares, a path outside the copy folders is refused, and so is a
			// twin that is someone else's
			assertFalse(WorkTwinUpdates.copied(user, TWIN, "public/files/Notes.txt"));
			assertThrows(IllegalArgumentException.class,
					() -> WorkTwinUpdates.copied(user, TWIN, "public/files/../twin-sources.json"));
			assertThrows(IllegalArgumentException.class, () -> WorkTwinUpdates.copied(user, "twin-two", chat));
		} finally {
			ThreadStore.setUser(onThread);
		}
		// the new copy is compared with its own time, on any server and however its file is dated later, and the
		// suggestion made for the old one is gone
		assertEquals(List.of(), WorkTwinUpdates.open(ownerId, ownerType));
		Copy pulled = new Copy(TWIN, chat, "chat", "19:abc@thread.v2", null, COPIED.plus(Duration.ofDays(2)));
		assertEquals(COPIED, only(WorkTwinUpdates.known(ownerId, ownerType, List.of(pulled))).copiedAt());
	}

	// ---- helpers ----

	private static List<Map<String, Object>> with(List<Map<String, Object>> messages, Map<String, Object> last) {
		List<Map<String, Object>> all = new ArrayList<>(messages);
		all.add(last);
		return all;
	}

	// a twin as the server hands one out: it answers only on a thread whose signed in user may read it, which a
	// job's pool thread has none of until the check names the owner
	private IProject twin(String id, String name) {
		IProject project = mock(IProject.class);
		when(project.getEngineId()).thenAnswer(call -> {
			if (ThreadStore.getUser() != user) {
				throw new SemossPixelException(
						"Project " + id + " does not exist or user does not have READ_ONLY permission on it");
			}
			return id;
		});
		when(project.getEngineName()).thenReturn(name);
		return project;
	}

	// the twins the owner's pages list as theirs
	@SafeVarargs
	private static MockedStatic<SecurityProjectUtils> owns(Map<String, Object>... twins) {
		return mockStatic(SecurityProjectUtils.class,
				call -> "getUserProjectList".equals(call.getMethod().getName()) ? List.of(twins) : null);
	}

	private CollaborationJobUtils.Job job() {
		CollaborationJobUtils.Job job = mock(CollaborationJobUtils.Job.class);
		when(job.ownerId()).thenReturn(ownerId);
		when(job.ownerType()).thenReturn(ownerType);
		return job;
	}

	// a twin's files as its pages leave them: each copy, written when the copies of these tests were, and the record
	// of what each came from
	private static void holds(Path assets, Map<String, Object> record) throws Exception {
		for (String path : record.keySet()) {
			if (path.startsWith("public/")) {
				Path copy = assets.resolve(path);
				Files.createDirectories(copy.getParent());
				Files.writeString(copy, "copy");
				Files.setLastModifiedTime(copy, FileTime.from(COPIED));
			}
		}
		Files.writeString(assets.resolve(WorkTwinUpdates.RECORD), CollaborationDbUtils.toJson(record));
	}

	// the server's clock as its rows keep it, UTC wall time (Utility.getCurrentSqlTimestampUTC)
	private static Timestamp utcNow() {
		return Timestamp.valueOf(LocalDateTime.now(ZoneOffset.UTC));
	}

	private Copy thread(String conversationId) {
		return new Copy(TWIN, THREAD, "email", "conversation:" + conversationId, null, COPIED);
	}

	private Copy chat(String chatId) {
		return new Copy(TWIN, "public/teams-chats/Chat - " + chatId.substring(3, 7) + ".md", "chat", chatId, null,
				COPIED);
	}

	private Copy file(String path, String itemId, String driveId) {
		return new Copy(TWIN, path, "file", itemId, driveId, COPIED);
	}

	private List<String> states(Copy... copies) {
		return WorkTwinUpdates.compare(List.of(copies), source, folders, null).stream().map(Finding::state).toList();
	}

	private Finding only(Copy copy) {
		return only(WorkTwinUpdates.compare(List.of(copy), source, folders, null));
	}

	private static <T> T only(List<T> items) {
		assertEquals(1, items.size());
		return items.get(0);
	}

	// an email as the mailbox lists it without its body
	private static Map<String, Object> email(String conversationId, String received, String from, String folder,
			boolean draft) {
		Map<String, Object> message = new LinkedHashMap<>();
		message.put("id", UUID.randomUUID().toString());
		message.put("conversationId", conversationId);
		message.put("subject", "Budget");
		message.put("from", Map.of("emailAddress", Map.of("name", from, "address", "someone@test.com")));
		message.put("receivedDateTime", received);
		message.put("parentFolderId", folder);
		message.put("isDraft", draft);
		return message;
	}

	// a chat message as the Teams helper describes it
	private static Map<String, Object> message(String created, String from, String type, boolean deleted) {
		Map<String, Object> message = new LinkedHashMap<>();
		message.put("id", UUID.randomUUID().toString());
		message.put("messageType", type);
		message.put("createdDateTime", created);
		message.put("isDeleted", deleted);
		if (from != null) {
			message.put("fromName", from);
		}
		return message;
	}
}
