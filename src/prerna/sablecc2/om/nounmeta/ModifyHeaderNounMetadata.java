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
package prerna.sablecc2.om.nounmeta;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;

public class ModifyHeaderNounMetadata extends NounMetadata {

  public ModifyHeaderNounMetadata(String frameName, String origHeader, String newHeader) {
    this.value = new HashMap<String, Object>();
    ((Map) this.value).put("frameName", frameName);
    ((Map) this.value).put("remove", new String[] {origHeader});
    ((Map) this.value).put("add", new String[] {newHeader});

    setConfig();
  }

  public ModifyHeaderNounMetadata(
      String frameName, List<String> origHeaders, List<String> newHeaders) {
    this.value = new HashMap<String, Object>();
    ((Map) this.value).put("frameName", frameName);
    ((Map) this.value).put("remove", origHeaders);
    ((Map) this.value).put("add", newHeaders);

    setConfig();
  }

  public ModifyHeaderNounMetadata(String frameName, String[] origHeaders, String[] newHeaders) {
    this.value = new HashMap<String, Object>();
    ((Map) this.value).put("frameName", frameName);
    ((Map) this.value).put("remove", origHeaders);
    ((Map) this.value).put("add", newHeaders);

    setConfig();
  }

  private void setConfig() {
    this.noun = PixelDataType.CONST_STRING;
    this.opType.add(PixelOperationType.MODIFY_HEADERS);
  }
}
