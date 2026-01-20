package prerna.reactor.playwright;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;

class GetRoomConversationHistoryReactorTest {

    private GetRoomConversationHistoryReactor reactor;
    private Insight insight;
    private User user;

    @BeforeEach
    void setup() {
        reactor = new GetRoomConversationHistoryReactor();
        insight = mock(Insight.class);
        user = mock(User.class);

        reactor.setInsight(insight);
        when(insight.getUser()).thenReturn(user);
    }

    @Test
    void testExecuteWithNullUser() {
        reactor.keyValue.put(ReactorKeysEnum.ROOM_ID.getKey(), "room123");
        when(insight.getUser()).thenReturn(null);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            reactor.execute();
        });

        assertEquals("You are not properly logged in", exception.getMessage());
    }

    @Test
    void testExecuteWithMissingRoomId() {
        reactor.keyValue.put(ReactorKeysEnum.ROOM_ID.getKey(), "");

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            reactor.execute();
        });

        assertEquals("Room id is required", exception.getMessage());
    }

    @Test
    void testExecuteWithNullRoomId() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            reactor.execute();
        });

        assertEquals("Required input(s) missing: roomId", exception.getMessage());
    }

    @Test
    void testParseIntWithValidInteger() throws Exception {
        Method method = GetRoomConversationHistoryReactor.class.getDeclaredMethod("parseInt", String.class, int.class);
        method.setAccessible(true);

        int result = (int) method.invoke(null, "42", 0);
        assertEquals(42, result);
    }

    @Test
    void testParseIntWithNullValue() throws Exception {
        Method method = GetRoomConversationHistoryReactor.class.getDeclaredMethod("parseInt", String.class, int.class);
        method.setAccessible(true);

        int result = (int) method.invoke(null, null, 10);
        assertEquals(10, result);
    }

    @Test
    void testParseIntWithEmptyString() throws Exception {
        Method method = GetRoomConversationHistoryReactor.class.getDeclaredMethod("parseInt", String.class, int.class);
        method.setAccessible(true);

        int result = (int) method.invoke(null, "   ", 15);
        assertEquals(15, result);
    }

    @Test
    void testParseIntWithInvalidNumber() throws Exception {
        Method method = GetRoomConversationHistoryReactor.class.getDeclaredMethod("parseInt", String.class, int.class);
        method.setAccessible(true);

        int result = (int) method.invoke(null, "abc", 20);
        assertEquals(20, result);
    }

    @Test
    void testParseIntWithNegativeNumber() throws Exception {
        Method method = GetRoomConversationHistoryReactor.class.getDeclaredMethod("parseInt", String.class, int.class);
        method.setAccessible(true);

        int result = (int) method.invoke(null, "-5", 0);
        assertEquals(-5, result);
    }

    @Test
    void testNormalizeSortWithAscending() throws Exception {
        Method method = GetRoomConversationHistoryReactor.class.getDeclaredMethod("normalizeSort", String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(null, "asc");
        assertEquals("ASC", result);
    }

    @Test
    void testNormalizeSortWithDescending() throws Exception {
        Method method = GetRoomConversationHistoryReactor.class.getDeclaredMethod("normalizeSort", String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(null, "desc");
        assertEquals("DESC", result);
    }

    @Test
    void testNormalizeSortWithMixedCase() throws Exception {
        Method method = GetRoomConversationHistoryReactor.class.getDeclaredMethod("normalizeSort", String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(null, "DeSc");
        assertEquals("DESC", result);
    }

    @Test
    void testNormalizeSortWithNull() throws Exception {
        Method method = GetRoomConversationHistoryReactor.class.getDeclaredMethod("normalizeSort", String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(null, (String) null);
        assertEquals("ASC", result);
    }

    @Test
    void testNormalizeSortWithInvalidValue() throws Exception {
        Method method = GetRoomConversationHistoryReactor.class.getDeclaredMethod("normalizeSort", String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(null, "invalid");
        assertEquals("ASC", result);
    }

    @Test
    void testApplyPagingWithNoOffset() throws Exception {
        Method method = GetRoomConversationHistoryReactor.class.getDeclaredMethod("applyPaging", List.class, int.class,
                int.class);
        method.setAccessible(true);

        List<Map<String, Object>> pairs = createTestPairs(5);
        List<Map<String, Object>> result = (List<Map<String, Object>>) method.invoke(null, pairs, 0, 3);

        assertEquals(3, result.size());
    }

    @Test
    void testApplyPagingWithOffset() throws Exception {
        Method method = GetRoomConversationHistoryReactor.class.getDeclaredMethod("applyPaging", List.class, int.class,
                int.class);
        method.setAccessible(true);

        List<Map<String, Object>> pairs = createTestPairs(10);
        List<Map<String, Object>> result = (List<Map<String, Object>>) method.invoke(null, pairs, 2, 3);

        assertEquals(3, result.size());
    }

    @Test
    void testApplyPagingWithNoLimit() throws Exception {
        Method method = GetRoomConversationHistoryReactor.class.getDeclaredMethod("applyPaging", List.class, int.class,
                int.class);
        method.setAccessible(true);

        List<Map<String, Object>> pairs = createTestPairs(5);
        List<Map<String, Object>> result = (List<Map<String, Object>>) method.invoke(null, pairs, 0, -1);

        assertEquals(5, result.size());
    }

    @Test
    void testApplyPagingWithOffsetBeyondSize() throws Exception {
        Method method = GetRoomConversationHistoryReactor.class.getDeclaredMethod("applyPaging", List.class, int.class,
                int.class);
        method.setAccessible(true);

        List<Map<String, Object>> pairs = createTestPairs(5);
        List<Map<String, Object>> result = (List<Map<String, Object>>) method.invoke(null, pairs, 10, 3);

        assertEquals(0, result.size());
    }

    @Test
    void testApplyPagingWithEmptyList() throws Exception {
        Method method = GetRoomConversationHistoryReactor.class.getDeclaredMethod("applyPaging", List.class, int.class,
                int.class);
        method.setAccessible(true);

        List<Map<String, Object>> pairs = new ArrayList<>();
        List<Map<String, Object>> result = (List<Map<String, Object>>) method.invoke(null, pairs, 0, 5);

        assertEquals(0, result.size());
    }

    @Test
    void testApplyPagingWithNegativeOffset() throws Exception {
        Method method = GetRoomConversationHistoryReactor.class.getDeclaredMethod("applyPaging", List.class, int.class,
                int.class);
        method.setAccessible(true);

        List<Map<String, Object>> pairs = createTestPairs(5);
        List<Map<String, Object>> result = (List<Map<String, Object>>) method.invoke(null, pairs, -5, 3);

        assertEquals(3, result.size());
    }

    @Test
    void testApplyPagingWithLimitExceedingSize() throws Exception {
        Method method = GetRoomConversationHistoryReactor.class.getDeclaredMethod("applyPaging", List.class, int.class,
                int.class);
        method.setAccessible(true);

        List<Map<String, Object>> pairs = createTestPairs(5);
        List<Map<String, Object>> result = (List<Map<String, Object>>) method.invoke(null, pairs, 0, 100);

        assertEquals(5, result.size());
    }

    @Test
    void testFirstNonBlankWithFirstNonBlank() throws Exception {
        Method method = GetRoomConversationHistoryReactor.class.getDeclaredMethod("firstNonBlank", String.class,
                String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(null, "first", "second");
        assertEquals("first", result);
    }

    @Test
    void testFirstNonBlankWithFirstBlank() throws Exception {
        Method method = GetRoomConversationHistoryReactor.class.getDeclaredMethod("firstNonBlank", String.class,
                String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(null, "  ", "second");
        assertEquals("second", result);
    }

    @Test
    void testFirstNonBlankWithBothBlank() throws Exception {
        Method method = GetRoomConversationHistoryReactor.class.getDeclaredMethod("firstNonBlank", String.class,
                String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(null, "  ", "   ");
        assertEquals(null, result);
    }

    @Test
    void testFirstNonBlankWithBothNull() throws Exception {
        Method method = GetRoomConversationHistoryReactor.class.getDeclaredMethod("firstNonBlank", String.class,
                String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(null, null, null);
        assertEquals(null, result);
    }

    @Test
    void testFirstNonBlankWithFirstNull() throws Exception {
        Method method = GetRoomConversationHistoryReactor.class.getDeclaredMethod("firstNonBlank", String.class,
                String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(null, null, "second");
        assertEquals("second", result);
    }

    @Test
    void testBuildHistoryStringWithValidPairs() throws Exception {
        Method method = GetRoomConversationHistoryReactor.class.getDeclaredMethod("buildHistoryString", List.class);
        method.setAccessible(true);

        List<Map<String, Object>> pairs = createTestPairs(2);
        String result = (String) method.invoke(null, pairs);

        assertNotNull(result);
        assertTrue(result.contains("User:"));
        assertTrue(result.contains("Assistant:"));
        assertTrue(result.contains("---"));
    }

    @Test
    void testBuildHistoryStringWithEmptyList() throws Exception {
        Method method = GetRoomConversationHistoryReactor.class.getDeclaredMethod("buildHistoryString", List.class);
        method.setAccessible(true);

        List<Map<String, Object>> pairs = new ArrayList<>();
        String result = (String) method.invoke(null, pairs);

        assertEquals("", result);
    }

    @Test
    void testBuildHistoryStringWithNullList() throws Exception {
        Method method = GetRoomConversationHistoryReactor.class.getDeclaredMethod("buildHistoryString", List.class);
        method.setAccessible(true);

        String result = (String) method.invoke(null, (List<Map<String, Object>>) null);

        assertEquals("", result);
    }

    @Test
    void testBuildHistoryStringWithSinglePair() throws Exception {
        Method method = GetRoomConversationHistoryReactor.class.getDeclaredMethod("buildHistoryString", List.class);
        method.setAccessible(true);

        List<Map<String, Object>> pairs = createTestPairs(1);
        String result = (String) method.invoke(null, pairs);

        assertNotNull(result);
        assertTrue(result.contains("User:"));
        assertTrue(result.contains("Assistant:"));
        assertTrue(!result.contains("---"));
    }

    @Test
    void testBuildHistoryStringWithNullQuestionAndAnswer() throws Exception {
        Method method = GetRoomConversationHistoryReactor.class.getDeclaredMethod("buildHistoryString", List.class);
        method.setAccessible(true);

        Map<String, Object> pair = new java.util.HashMap<>();
        pair.put("question", null);
        pair.put("answer", null);
        List<Map<String, Object>> pairs = Arrays.asList(pair);
        String result = (String) method.invoke(null, pairs);

        assertNotNull(result);
        assertEquals("User: \nAssistant: ", result);
    }

    @Test
    void testGetReactorDescription() {
        String description = reactor.getReactorDescription();

        assertNotNull(description);
        assertTrue(description.contains("conversation history"));
    }

    @Test
    void testGetDescriptionForRoomIdKey() {
        String description = reactor.getDescriptionForKey(ReactorKeysEnum.ROOM_ID.getKey());
        assertNotNull(description);
        assertTrue(description.contains("room"));
    }

    @Test
    void testGetDescriptionForLimitKey() {
        String description = reactor.getDescriptionForKey(ReactorKeysEnum.LIMIT.getKey());
        assertNotNull(description);
        assertTrue(description.contains("Maximum"));
    }

    @Test
    void testGetDescriptionForOffsetKey() {
        String description = reactor.getDescriptionForKey(ReactorKeysEnum.OFFSET.getKey());
        assertNotNull(description);
        assertTrue(description.contains("skip"));
    }

    @Test
    void testGetDescriptionForSortKey() {
        String description = reactor.getDescriptionForKey(ReactorKeysEnum.SORT.getKey());
        assertNotNull(description);
        assertTrue(description.contains("Sort"));
    }

    @Test
    void testGetDescriptionForIncludePartialKey() {
        String description = reactor.getDescriptionForKey("includePartial");
        assertNotNull(description);
        assertTrue(description.contains("unanswered"));
    }

    private List<Map<String, Object>> createTestPairs(int count) {
        List<Map<String, Object>> pairs = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            pairs.add(Map.of("question", "Question " + i, "answer", "Answer " + i));
        }
        return pairs;
    }
}
