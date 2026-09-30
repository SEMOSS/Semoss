package prerna.collaboration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** Regression coverage for older source history and last-scanned continuation. */
class BrainThreadPagingUnitTests {
    private Map<String,Object> read(int count, String cursor, Set<Integer> hidden, Set<Integer> gone, boolean missing, AtomicInteger fetches) {
        try (MockedStatic<CollaborationDbUtils> db = mockStatic(CollaborationDbUtils.class, invocation -> {
            String method = invocation.getMethod().getName();
            if (method.equals("queryOne")) return missing ? null : new String[]{"outlook", "outlook:conversation", "false"};
            if (method.equals("query")) {
                String sql = invocation.getArgument(0);
                if (!sql.contains("FROM BRAIN_MESSAGE")) return List.of();
                assertTrue(sql.contains("OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ?"));
                List<Object> rows = new ArrayList<>();
                CollaborationDbUtils.RowMapper<?> mapper = invocation.getArgument(1);
                for (int i = 0; i < count; i++) {
                    ResultSet rs = mock(ResultSet.class);
                    when(rs.getString("MESSAGE_KEY")).thenReturn(String.format("key-%04d", i));
                    when(rs.getString("GRAPH_ID")).thenReturn("id-" + i);
                    when(rs.getString("SENDER_PERSON_ID")).thenReturn("person");
                    when(rs.getString("DECISION")).thenReturn(hidden.contains(i) ? BrainRulesGate.NEVER : "include");
                    rows.add(mapper.map(rs));
                }
                return rows;
            }
            if (method.equals("getTimestamp")) return "2026-09-30T10:00:00Z";
            return invocation.callRealMethod();
        }); MockedStatic<BrainRulesGate> rules = mockStatic(BrainRulesGate.class, invocation -> {
            if (invocation.getMethod().getName().equals("activeRules")) return List.of();
            return invocation.callRealMethod();
        })) {
            BrainMessageSource source = (user, channel, conversation, id) -> {
                fetches.incrementAndGet();
                if (gone.contains(Integer.parseInt(id.substring(3)))) return null;
                return Map.of("conversationId", "conversation", "subject", id,
                    "body", Map.of("contentType", "html", "content", "<p>" + id + "</p>"),
                    "uniqueBody", Map.of("contentType", "text", "content", id));
            };
            return BrainThreadMessages.read(null, "owner", "type", "thread", 100, source, true, cursor);
        }
    }
    @SuppressWarnings("unchecked")
    private List<Map<String,Object>> messages(Map<String,Object> page) { return (List<Map<String,Object>>)page.get("messages"); }

    @Test void readsEveryLinkedMessageAcrossPagesWithEqualTimestamps() {
        AtomicInteger fetches = new AtomicInteger();
        Map<String,Object> first = read(125, null, Set.of(), Set.of(), false, fetches);
        assertEquals(100, messages(first).size());
        assertEquals(true, first.get("hasMore"));
        assertEquals("id-99", messages(first).get(0).get("id"));
        Map<String,Object> older = read(125, (String)first.get("nextCursor"), Set.of(), Set.of(), false, fetches);
        assertEquals(25, messages(older).size());
        assertEquals(false, older.get("hasMore"));
        assertFalse(older.containsKey("nextCursor"));
        Set<Object> ids = new HashSet<>();
        messages(first).forEach(item -> assertTrue(ids.add(item.get("id"))));
        messages(older).forEach(item -> assertTrue(ids.add(item.get("id"))));
        assertEquals(125, ids.size());
        assertEquals(125, fetches.get());
    }
    @Test void continuesPastUnavailableAndNowHiddenAnchorRecords() {
        AtomicInteger fetches = new AtomicInteger();
        Map<String,Object> first = read(125, null, Set.of(1), Set.of(0, 2), false, fetches);
        assertEquals(100, messages(first).size());
        assertEquals(2, first.get("unavailableCount"));
        Map<String,Object> older = read(125, (String)first.get("nextCursor"), Set.of(1, 102, 110), Set.of(0, 2), false, fetches);
        assertEquals(21, messages(older).size());
        assertFalse(messages(older).stream().anyMatch(item -> item.get("id").equals("id-110")));
        assertEquals(false, older.get("hasMore"));
    }
    @Test void invalidCrossThreadAndDeletedAnchorsFailWithoutFetching() {
        AtomicInteger fetches = new AtomicInteger();
        assertThrows(IllegalArgumentException.class, () -> read(125, "not-valid!", Set.of(), Set.of(), false, fetches));
        String other = java.util.Base64.getUrlEncoder().encodeToString("other\nkey-0001".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertThrows(IllegalArgumentException.class, () -> read(125, other, Set.of(), Set.of(), false, fetches));
        String deleted = java.util.Base64.getUrlEncoder().encodeToString("thread\nkey-9999".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertThrows(IllegalArgumentException.class, () -> read(125, deleted, Set.of(), Set.of(), false, fetches));
        assertThrows(IllegalArgumentException.class, () -> read(125, null, Set.of(), Set.of(), true, fetches));
        assertEquals(0, fetches.get());
    }
}
