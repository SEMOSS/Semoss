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

import org.junit.jupiter.api.Test;
import prerna.date.SemossDate;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class TimestampReactorUnitTests {
  TimestampReactor reactor;

  @Test
  void test() {
    reactor = new TimestampReactor();
    reactor.keyValue.put("date", "2025-01-01");
    reactor.keyValue.put("format", "yyyy-MM-dd");

    NounMetadata nm = reactor.execute();
    assertEquals(PixelDataType.CONST_DATE, nm.getNounType());
    assertInstanceOf(SemossDate.class, nm.getValue());

    reactor.keyValue.remove("date");
    nm = reactor.execute();
    assertEquals(PixelDataType.CONST_DATE, nm.getNounType());
    assertInstanceOf(SemossDate.class, nm.getValue());
  }
}
