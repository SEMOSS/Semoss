/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.engine.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class CaseInsensitivePropertiesUnitTests {

  private CaseInsensitiveProperties cips;

  @BeforeEach
  public void setup() {
    cips = new CaseInsensitiveProperties();
  }

  // POTENTIAL BUG: SHOULDN'T THE PROPERTIES BE UPPERCASED HERE?
  @Test
  void testCaseInsensitiveProperties() {
    Properties props = new Properties();
    props.setProperty("a", "b");
    props.setProperty("a2", "b2");
    cips = new CaseInsensitiveProperties(props);
    Set<Object> keyset = cips.keySet();
    assertTrue(keyset.contains("a"));
    assertTrue(keyset.contains("a2"));
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
    return Stream.of(Arguments.of("a"), Arguments.of(1));
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
