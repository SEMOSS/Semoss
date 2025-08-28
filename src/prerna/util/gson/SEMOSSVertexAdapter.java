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
package prerna.util.gson;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;
import prerna.om.SEMOSSVertex;

public class SEMOSSVertexAdapter extends TypeAdapter<SEMOSSVertex> {

  @Override
  public void write(JsonWriter out, SEMOSSVertex value) throws IOException {
    String uri = value.getURI();
    Hashtable propHash = value.getProperty();

    out.beginObject();
    out.name("uri").value(uri);
    out.name("propHash");
    out.beginObject();
    // flush out properties
    for (Object key : propHash.keySet()) {
      out.name(key.toString());
      Object innerObj = propHash.get(key);
      writePropHash(out, innerObj);
    }
    out.endObject();
    out.endObject();
  }

  private void writePropHash(JsonWriter out, Object obj) throws IOException {
    if (obj instanceof Map) {
      out.beginObject();
      Map mapObj = (Map) obj;
      for (Object key : mapObj.keySet()) {
        out.name(key.toString());
        Object value = mapObj.get(key);
        if (value instanceof Map) {
          // make it recursive
          writePropHash(out, value);
        } else if (value instanceof Number) {
          out.value((Number) value);
        } else {
          out.value(value.toString());
        }
      }
      out.endObject();
    } else if (obj instanceof Number) {
      Number num = (Number) obj;
      if (Double.isNaN(num.doubleValue())) {
        out.value(0);
      } else {
        out.value((Number) obj);
      }
    } else {
      out.value(obj.toString());
    }
  }

  @Override
  public SEMOSSVertex read(JsonReader in) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }
}
