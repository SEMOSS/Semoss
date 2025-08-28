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
package prerna.date.reactor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DateManipulationReactorUnitTests {
  DateManipulationReactor reactor;

  @BeforeEach
  void setUp() {
    reactor = new DateManipulationReactor();
  }

  @Test
  void test() {
    NounMetadata nm = reactor.execute();
    assertEquals(PixelDataType.ERROR, nm.getNounType());
    assertEquals(PixelOperationType.ERROR, nm.getOpType().get(0));
    assertEquals("Must provide the 'type' of date manipulation", nm.getValue());

    reactor.keyValue.put("type", "add");
    reactor.keyValue.put("recurrence", "1");
    reactor.keyValue.put("timeunit", "days");

    SemossPixelException e = assertThrows(SemossPixelException.class, () -> reactor.execute());
    assertEquals("Starting Date must be set", e.getMessage());

    reactor.keyValue.put("date", "2025-01-01");
    nm = reactor.execute();
    assertEquals(PixelDataType.CONST_DATE, nm.getNounType());
    assertInstanceOf(SemossDate.class, nm.getValue());

    reactor.keyValue.replace("type", "subtract");
    nm = reactor.execute();
    assertEquals(PixelDataType.CONST_DATE, nm.getNounType());
    assertInstanceOf(SemossDate.class, nm.getValue());

    reactor.keyValue.replace("type", "diff");
    reactor.keyValue.replace("recurrence", "2025-12-31");
    nm = reactor.execute();
    assertEquals(PixelDataType.CONST_INT, nm.getNounType());
    assertInstanceOf(Integer.class, nm.getValue());

    reactor.keyValue.replace("type", "add");
    reactor.keyValue.replace("recurrence", "x");
    nm = reactor.execute();
    assertEquals(PixelDataType.CONST_DATE, nm.getNounType());
    assertInstanceOf(SemossDate.class, nm.getValue());

    reactor.keyValue.replace("type", "add");
    reactor.keyValue.replace("recurrence", "x");
    nm = reactor.execute();
    assertEquals(PixelDataType.CONST_DATE, nm.getNounType());
    assertInstanceOf(SemossDate.class, nm.getValue());

    reactor.keyValue.replace("type", "mult");
    nm = reactor.execute();
    assertEquals(PixelDataType.ERROR, nm.getNounType());
    assertEquals(PixelOperationType.ERROR, nm.getOpType().get(0));
    assertEquals("Unknown type = 'mult' for the date manipulation", nm.getValue());

    assertTrue(reactor.canMergeIntoQs());
  }

  @Test
  void mergeIntoQsMetadata() {
    Map<String, Object> expected = new HashMap<>();
    expected.put("qsMergeFormat", "scalar");
    expected.put("qsMergeDataType", SemossDataType.DATE);

    reactor.keyValue.put("date", "2025-01-01");
    reactor.keyValue.put("recurrence", "1");
    reactor.keyValue.put("timeunit", "days");

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> reactor.mergeIntoQsMetadata());
    assertEquals("Must provide the 'type' of date manipulation", e.getMessage());

    reactor.keyValue.put("type", "add");
    assertEquals(expected, reactor.mergeIntoQsMetadata());

    reactor.keyValue.replace("type", "diff");
    expected.replace("qsMergeDataType", SemossDataType.INT);
    assertEquals(expected, reactor.mergeIntoQsMetadata());

    reactor.keyValue.replace("type", "mult");
    e = assertThrows(IllegalArgumentException.class, () -> reactor.mergeIntoQsMetadata());
    assertEquals("Unknown type = 'mult' for the date manipulation", e.getMessage());
  }
}
