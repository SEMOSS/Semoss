/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.util.gson;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.options.TaskOptions;

public class BasicIteratorTaskAdapter extends AbstractSemossTypeAdapter<BasicIteratorTask> {

  static enum MODE {
    RECREATE_NEW,
    CONTINUE_PREVIOUS_ITERATING
  }

  private MODE curMode = MODE.RECREATE_NEW;

  @Override
  public BasicIteratorTask read(JsonReader in) throws IOException {
    String taskId = null;
    int numCollected = 0;
    long internalOffset = 0;
    TaskOptions tOptions = null;
    SelectQueryStruct qs = null;

    in.beginObject();
    while (in.hasNext()) {
      String key = in.nextName();
      JsonToken peek = in.peek();
      if (peek == JsonToken.NULL) {
        in.nextNull();
        continue;
      }

      if (key.equals("taskType")) {
        in.nextString();
      } else if (key.equals("id")) {
        taskId = in.nextString();
      } else if (key.equals("numCollected")) {
        numCollected = (int) in.nextLong();
      } else if (key.equals("internalOffset")) {
        internalOffset = in.nextLong();
      } else if (key.equals("taskOptions")) {
        TaskOptionsAdapter adapter = new TaskOptionsAdapter();
        tOptions = (TaskOptions) adapter.read(in);
      } else if (key.equals("qs")) {
        SelectQueryStructAdapter adapter = new SelectQueryStructAdapter();
        adapter.setInsight(this.insight);
        qs = adapter.read(in);
      }
    }
    in.endObject();

    BasicIteratorTask task = new BasicIteratorTask(qs);
    task.setId(taskId);
    task.setTaskOptions(tOptions);
    task.setNumCollect(numCollected);
    task.setInternalOffset(internalOffset);
    if (curMode == MODE.CONTINUE_PREVIOUS_ITERATING) {
      task.setInternalOffset(internalOffset + numCollected);
    }

    return task;
  }

  @Override
  public void write(JsonWriter out, BasicIteratorTask value) throws IOException {
    out.beginObject();
    out.name("taskType").value("basic");
    out.name("id").value(value.getId());
    out.name("numCollected").value(value.getNumCollect());
    out.name("internalOffset").value(value.getInternalOffset());
    out.name("taskOptions");
    if (value.getTaskOptions() != null) {
      TaskOptionsAdapter adapter = new TaskOptionsAdapter();
      adapter.write(out, value.getTaskOptions());
    } else {
      out.nullValue();
    }
    out.name("qs");
    SelectQueryStruct qs = value.getQueryStruct();
    SelectQueryStructAdapter adapter = new SelectQueryStructAdapter();
    adapter.write(out, qs);
    out.endObject();
  }

  public void setCurMode(MODE curMode) {
    this.curMode = curMode;
  }
}
