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

import java.util.Calendar;
import prerna.date.SemossDate;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class TimestampReactor extends AbstractReactor {

  private static final String DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";

  public TimestampReactor() {
    this.keysToGet = new String[] {"date", "format"};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    SemossDate date = null;
    String pattern = DEFAULT_FORMAT;

    /*
     * If there is no date input, then we will grab todays date
     * If there is a date input, we assume it is yyyy-MM-dd format
     * If there is a date input and a format, we will use that format
     */

    // determine if we should use the default format
    // or the user defined format
    if (this.keyValue.containsKey(this.keysToGet[1])) {
      pattern = this.keyValue.get(this.keysToGet[1]);
    }

    if (this.keyValue.containsKey(this.keysToGet[0])) {
      String strDate = this.keyValue.get(this.keysToGet[0]);

      date = new SemossDate(strDate, pattern);
      date.getZonedDateTime();
    } else {
      // the user hasn't specified a date
      date = new SemossDate(Calendar.getInstance().getTime(), pattern);
    }

    return new NounMetadata(date, PixelDataType.CONST_DATE);
  }
}
