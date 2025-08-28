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
package prerna.testing.date.reactor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import org.junit.jupiter.api.Test;
import prerna.date.SemossDate;
import prerna.date.reactor.DateReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestUtils;

public class DateReactorApiTests extends AbstractBaseSemossApiTests {

  @Test
  public void getDate() {
    String pixel = ApiSemossTestUtils.buildPixelCall(DateReactor.class, "date", "2022-03-19");
    NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
    String date = nm.getValue().toString();
    assertEquals("2022-03-19", date);
  }

  @Test
  public void getDate2() {
    // sanity check to make sure multiple tests run fine :)
    String pixel = ApiSemossTestUtils.buildPixelCall(DateReactor.class, "date", "2022-03-19");
    NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
    String date = nm.getValue().toString();
    assertEquals("2022-03-19", date);
  }

  @Test
  public void getDateWithCustomFormat() {
    String pixel = ApiSemossTestUtils.buildPixelCall(DateReactor.class, "format", "dd/MM/yyyy");
    NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
    SemossDate sd = (SemossDate) nm.getValue();
    assertTrue(sd.getFormattedDate().contains("/"));
    assertFalse(sd.getFormattedDate().contains("-"));
  }

  @Test
  public void getDateWithNoInput() {
    String pixel = ApiSemossTestUtils.buildPixelCall(DateReactor.class);
    NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
    String date = nm.getValue().toString();
    String today = new SimpleDateFormat("yyyy-MM-dd").format(Calendar.getInstance().getTime());
    assertEquals(today, date);
  }
}
