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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import prerna.auth.User;

class BrainAttachmentsUnitTests {

    private static final long MB = 1024L * 1024L;

    @TempDir
    Path dir;

    /** A thread with one email ("native-id") from Jane, carrying the given attachment. */
    private static final class FakeSource implements BrainMessageSource {
        final AtomicInteger fetches = new AtomicInteger();
        final AtomicInteger downloads = new AtomicInteger();
        final Map<String, Object> attachment;
        final byte[] bytes;
        RuntimeException listingError;

        FakeSource(Map<String, Object> attachment, byte[] bytes) {
            this.attachment = attachment;
            this.bytes = bytes;
        }

        @Override
        public Map<String, Object> fetch(User user, String source, String conversationId, String graphId) {
            fetches.incrementAndGet();
            return Map.of("subject", "Q3 budget", "hasAttachments", true,
                    "from", Map.of("emailAddress", Map.of("name", "Jane Doe", "address", "jane@example.com")),
                    "body", Map.of("contentType", "text", "content", "See attached."),
                    "uniqueBody", Map.of("contentType", "text", "content", "See attached."));
        }

        @Override
        public Map<String, List<Map<String, Object>>> attachments(User user, String source, List<String> graphIds) {
            if (listingError != null) {
                throw listingError;
            }
            return Map.of("native-id", List.of(BrainAttachments.describe(attachment)));
        }

        @Override
        public Map<String, Object> attachment(User user, String source, String graphId, String attachmentId) {
            return attachmentId.equals(attachment.get("id")) ? attachment : null;
        }

        @Override
        public Path download(User user, String source, String graphId, String attachmentId, Path dir, String fileName)
                throws Exception {
            downloads.incrementAndGet();
            Path file = dir.resolve(fileName);
            Files.write(file, bytes);
            return file;
        }
    }

    private static Map<String, Object> file(String name, long size) {
        return Map.of("@odata.type", "#microsoft.graph.fileAttachment", "id", "att-1", "name", name, "size", size,
                "contentType", "application/octet-stream", "isInline", false);
    }

    /** Runs body with the thread's database rows and rules mocked; never is the never-ingest answer. */
    private <T> T withThread(String graphId, boolean never, Supplier<T> body) {
        try (MockedStatic<CollaborationDbUtils> db = mockStatic(CollaborationDbUtils.class, invocation -> {
            String method = invocation.getMethod().getName();
            if (method.equals("queryOne")) {
                String sql = invocation.getArgument(0);
                return sql.startsWith("SELECT SOURCE FROM") ? "email"
                        : new String[] { "email", "email:conversation", "false" };
            }
            if (method.equals("query")) {
                String sql = invocation.getArgument(0);
                if (!sql.contains("FROM BRAIN_MESSAGE")) return List.of();
                ResultSet rs = mock(ResultSet.class);
                when(rs.getString("MESSAGE_KEY")).thenReturn("key-1");
                when(rs.getString("GRAPH_ID")).thenReturn(graphId);
                when(rs.getString("SENDER_PERSON_ID")).thenReturn("p-jane");
                when(rs.getString("DECISION")).thenReturn(BrainRulesGate.INGESTED);
                CollaborationDbUtils.RowMapper<?> mapper = invocation.getArgument(1);
                return List.of(mapper.map(rs));
            }
            if (method.equals("getTimestamp")) return "2026-09-25T12:00:00Z";
            return invocation.callRealMethod();
        }); MockedStatic<BrainRulesGate> rules = mockStatic(BrainRulesGate.class, invocation -> {
            String method = invocation.getMethod().getName();
            if (method.equals("activeRules")) return List.of();
            if (method.equals("neverRule"))
                return never ? new BrainRulesGate.Rule("rule", "never_sender", "jane@example.com", null, null, null)
                        : null;
            if (method.equals("keywordRule")) return null;
            return invocation.callRealMethod();
        })) {
            return body.get();
        }
    }

    private Map<String, Object> stage(FakeSource source, String fileName, boolean includeText, long maxBytes) {
        return withThread("native-id", false, () -> BrainAttachments.stage(null, "owner", "type", dir.toString(),
                "thread", "native-id", "att-1", fileName, includeText, source, maxBytes));
    }

    private static byte[] docx(String text) throws Exception {
        try (XWPFDocument doc = new XWPFDocument(); ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            doc.createParagraph().createRun().setText(text);
            doc.write(out);
            return out.toByteArray();
        }
    }

    @Test
    void stagesTheFileAndATextCopyNamedAfterIt() throws Exception {
        byte[] bytes = docx("Budget line one");
        FakeSource source = new FakeSource(file("Budget.docx", bytes.length), bytes);
        Map<String, Object> result = stage(source, "Budget-a1b2c3.docx", true, 10 * MB);

        assertEquals("Budget-a1b2c3.docx", result.get("filePath"));
        assertEquals("Budget.docx", result.get("name"));
        assertEquals((long) bytes.length, result.get("size"));
        assertArrayEquals(bytes, Files.readAllBytes(dir.resolve("Budget-a1b2c3.docx")));
        assertEquals("Budget-a1b2c3.docx.txt", result.get("textPath"));
        String text = Files.readString(dir.resolve("Budget-a1b2c3.docx.txt"), StandardCharsets.UTF_8);
        assertTrue(text.startsWith("Attachment: Budget.docx\nFrom the email \"Q3 budget\" sent by Jane Doe"), text);
        assertTrue(text.contains("Budget line one"), text);
        // the temporary download name never survives
        try (var files = Files.list(dir)) {
            assertEquals(2, files.count());
        }
    }

    @Test
    void noTextCopyUnlessAskedOrReadable() throws Exception {
        byte[] bytes = docx("Budget line one");
        Map<String, Object> plain = stage(new FakeSource(file("Budget.docx", bytes.length), bytes), "a.docx", false,
                10 * MB);
        assertFalse(plain.containsKey("textPath"));
        Map<String, Object> pdf = stage(new FakeSource(file("Report.pdf", 4), "%PDF".getBytes()), "b.pdf", true,
                10 * MB);
        assertFalse(pdf.containsKey("textPath"));
        Map<String, Object> broken = stage(new FakeSource(file("Broken.docx", 3), "abc".getBytes()), "c.docx", true,
                10 * MB);
        assertFalse(broken.containsKey("textPath"));
        assertEquals("The text of this file could not be read.", broken.get("textError"));
        assertTrue(Files.exists(dir.resolve("c.docx")));
    }

    @Test
    void neverIngestSenderIsRefusedBeforeAnyFetch() {
        FakeSource source = new FakeSource(file("Budget.docx", 10), new byte[10]);
        assertThrows(IllegalArgumentException.class, () -> withThread("native-id", true,
                () -> BrainAttachments.stage(null, "owner", "type", dir.toString(), "thread", "native-id", "att-1",
                        "a.docx", true, source, 10 * MB)));
        assertEquals(0, source.fetches.get());
        assertEquals(0, source.downloads.get());
    }

    @Test
    void anEmailThatIsNotOnTheThreadIsRefused() {
        FakeSource source = new FakeSource(file("Budget.docx", 10), new byte[10]);
        assertThrows(IllegalArgumentException.class, () -> withThread("other-id", false,
                () -> BrainAttachments.stage(null, "owner", "type", dir.toString(), "thread", "native-id", "att-1",
                        "a.docx", true, source, 10 * MB)));
        assertEquals(0, source.downloads.get());
    }

    @Test
    void aMessageIdWithALetterInTheWrongCaseStillFindsTheEmail() {
        FakeSource source = new FakeSource(file("Budget.docx", 10), new byte[10]);
        Map<String, Object> result = withThread("native-id", false,
                () -> BrainAttachments.stage(null, "owner", "type", dir.toString(), "thread", "Native-Id", "att-1",
                        "a.docx", false, source, 10 * MB));
        assertEquals("native-id", result.get("messageId"), "later calls use the stored id");
        assertEquals(1, source.downloads.get());
    }

    @Test
    void aGarbledMessageIdIsIgnoredWhenAThreadEmailCarriesThatFile() {
        FakeSource source = new FakeSource(file("Proposal 1.pdf", 10), new byte[10]);
        Map<String, Object> result = withThread("native-id", false, () -> BrainAttachments.stage(null, "owner",
                "type", dir.toString(), "thread", "AQMk-garbled-LY", null, "Proposal_1.pdf", null, false, source,
                10 * MB));
        assertEquals("native-id", result.get("messageId"));
        assertEquals(1, source.downloads.get());
    }

    @Test
    void aRetypedAttachmentNameStillMatchesWhenOnlySeparatorsAndCaseDiffer() {
        FakeSource source = new FakeSource(file("Q3_Budget Final.docx", 10), new byte[10]);
        withThread("native-id", false, () -> BrainAttachments.stage(null, "owner", "type", dir.toString(), "thread",
                "native-id", null, "q3 budget-final.docx", null, false, source, 10 * MB));
        assertEquals(1, source.downloads.get());
    }

    @Test
    void aListedSizeOverTheCapIsRefusedBeforeDownloading() {
        FakeSource source = new FakeSource(file("Huge.pdf", 11 * MB), new byte[10]);
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> stage(source, "huge.pdf", false, 10 * MB));
        assertEquals("Huge.pdf is larger than the 10 MB limit.", error.getMessage());
        assertEquals(0, source.downloads.get());
    }

    @Test
    void aDownloadOverTheCapIsDeleted() throws Exception {
        FakeSource source = new FakeSource(file("Small.pdf", 10), new byte[(int) MB + 1]);
        assertThrows(IllegalArgumentException.class, () -> stage(source, "small.pdf", false, MB));
        try (var files = Files.list(dir)) {
            assertEquals(0, files.count());
        }
    }

    @Test
    void linksAndAttachedEmailsAreNotFiles() {
        Map<String, Object> link = Map.of("@odata.type", "#microsoft.graph.referenceAttachment", "id", "att-1",
                "name", "Plan.docx", "size", 10);
        FakeSource source = new FakeSource(link, new byte[10]);
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> stage(source, "plan.docx", false, 10 * MB));
        assertTrue(error.getMessage().contains("Open Plan.docx in Outlook"));
        assertEquals(0, source.downloads.get());
    }

    @Test
    void aRequestedNameCannotLeaveTheFolderOrHide() throws Exception {
        FakeSource source = new FakeSource(file("x.pdf", 4), "%PDF".getBytes());
        assertEquals("evil.pdf", stage(source, "../../evil.pdf", false, 10 * MB).get("filePath"));
        assertTrue(Files.exists(dir.resolve("evil.pdf")));
        assertEquals("semoss", BrainAttachments.baseName(".semoss"));
        assertNull(BrainAttachments.baseName("../.."));
        assertEquals("x.pdf", stage(source, null, false, 10 * MB).get("filePath"));
    }

    @Test
    void describeNeverCarriesBytesAndNamesTheKind() {
        Map<String, Object> described = BrainAttachments.describe(Map.of("@odata.type",
                "#microsoft.graph.fileAttachment", "id", "a", "name", "f.pdf", "size", 12.0, "contentBytes", "AAAA"));
        assertEquals("file", described.get("kind"));
        assertEquals(12L, described.get("size"));
        assertFalse(described.containsKey("contentBytes"));
        assertEquals("item",
                BrainAttachments.describe(Map.of("@odata.type", "#microsoft.graph.itemAttachment")).get("kind"));
        assertEquals("link",
                BrainAttachments.describe(Map.of("@odata.type", "#microsoft.graph.referenceAttachment")).get("kind"));
    }

    @Test
    void theThreadReadListsAttachmentsOnlyWhenAsked() {
        FakeSource source = new FakeSource(file("Budget.docx", 10), new byte[10]);
        List<?> without = (List<?>) withThread("native-id", false, () -> BrainThreadMessages.read(null, "owner",
                "type", "thread", 20, source, true, null, false)).get("messages");
        assertFalse(((Map<?, ?>) without.get(0)).containsKey("attachments"));

        List<?> with = (List<?>) withThread("native-id", false, () -> BrainThreadMessages.read(null, "owner", "type",
                "thread", 20, source, true, null, true)).get("messages");
        List<?> attachments = (List<?>) ((Map<?, ?>) with.get(0)).get("attachments");
        assertEquals(1, attachments.size());
        assertEquals("Budget.docx", ((Map<?, ?>) attachments.get(0)).get("name"));
    }

    @Test
    void aFailedListingStillReadsTheThread() {
        FakeSource source = new FakeSource(file("Budget.docx", 10), new byte[10]);
        source.listingError = new IllegalStateException("Graph returned 500");
        List<?> messages = (List<?>) withThread("native-id", false, () -> BrainThreadMessages.read(null, "owner",
                "type", "thread", 20, source, true, null, true)).get("messages");
        assertEquals(1, messages.size());
        assertFalse(((Map<?, ?>) messages.get(0)).containsKey("attachments"));
    }
}
