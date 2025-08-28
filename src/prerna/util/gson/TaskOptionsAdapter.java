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

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.util.Map;
import prerna.sablecc2.om.task.options.TaskOptions;

public class TaskOptionsAdapter extends AbstractSemossTypeAdapter<TaskOptions> {

  private static Gson GSON = GsonUtility.getDefaultGson();

  @Override
  public TaskOptions read(JsonReader in) throws IOException {
    if (in.peek() == JsonToken.NULL) {
      return null;
    }

    Map<String, Object> options = null;
    boolean isOrnament = false;

    in.beginObject();
    while (in.hasNext()) {
      String name = in.nextName();
      if (in.peek() == JsonToken.NULL) {
        in.nextNull();
        continue;
      }

      if (name.equals("options")) {
        TypeAdapter adapter = GSON.getAdapter(Map.class);
        options = (Map<String, Object>) adapter.read(in);

      } else if (name.equals("ornament")) {
        isOrnament = in.nextBoolean();
      }
    }
    in.endObject();

    TaskOptions taskOptions = new TaskOptions(options);
    taskOptions.setOrnament(isOrnament);
    return taskOptions;
  }

  @Override
  public void write(JsonWriter out, TaskOptions value) throws IOException {
    if (value == null) {
      out.nullValue();
      return;
    }

    out.beginObject();
    out.name("options");
    Map<String, Object> options = value.getOptions();
    if (options == null) {
      out.nullValue();
    } else {
      TypeAdapter adapter = GSON.getAdapter(options.getClass());
      adapter.write(out, options);
    }
    out.name("ornament");
    out.value(value.isOrnament());

    out.endObject();
  }
}
