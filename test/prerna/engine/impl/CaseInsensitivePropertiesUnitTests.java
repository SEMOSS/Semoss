package prerna.engine.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CaseInsensitivePropertiesUnitTests {

    private CaseInsensitiveProperties cips;

    @BeforeEach
    public void setup() {
        cips = new CaseInsensitiveProperties();
    }

    @Test
    void testCaseInsensitiveProperties() {
        Properties props = new Properties();
        props.setProperty("a", "b");
        props.setProperty("a2", "b2");
        cips = new CaseInsensitiveProperties(props);
        Set<Object> keyset = cips.keySet();
        assertTrue(keyset.contains("A"));
        assertTrue(keyset.contains("A2"));
    }

    @Test
    void testSetProperty() {
        cips.setProperty("a", "b");
        assertEquals("b", cips.getProperty("A"));
    }

    @Test
    void testPutProperty() {
        cips.put("a", "b");
        assertEquals("b", cips.getProperty("A"));
    }

    @Test
    void testPutPropertyNonString() {
        cips.put(1, 2);
        assertEquals(2, cips.get(1));
    }

    @Test
    void testPutIfAbsent() {
        cips.put("a", "b");
        cips.putIfAbsent("a", "c");
        cips.put(1, "b");
        cips.putIfAbsent(1, "c");
        assertEquals("b", cips.get("A"));
        assertEquals("b", cips.get(1));
    }

    @Test
    void getPropertyDefault() {
        assertEquals("c", cips.getProperty("a", "c"));
    }

    static Stream<Arguments> inputProvider() {
        return Stream.of(
                Arguments.of("a"),
                Arguments.of(1)
        );
    }

    @ParameterizedTest
    @MethodSource("inputProvider")
    void getOrDefault(Object test) {
        assertEquals("c", cips.getOrDefault(test, "c"));
    }

    @ParameterizedTest
    @MethodSource("inputProvider")
    void containsKey(Object test) {
        cips.put("A", "b");
        cips.put(1, "b");
        assertTrue(cips.containsKey(test));
    }
}
