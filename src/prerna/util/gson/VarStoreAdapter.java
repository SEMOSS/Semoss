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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.insight.InsightUtility;

public class VarStoreAdapter extends TypeAdapter<VarStore> {

  private Set<String> keysToIgnore = new HashSet<String>();

  private boolean collectFrames = false;
  private List<FrameCacheHelper> frames = null;

  @Override
  public void write(JsonWriter out, VarStore value) throws IOException {
    out.beginObject();

    Set<String> keys = value.getKeys();

    // we will go through all the normal keys
    // and ignore the frames / tasks for the time being

    if (collectFrames) {
      for (String k : keys) {
        NounMetadata noun = value.get(k);
        if (noun.getNounType() == PixelDataType.TASK) {
          continue;
        } else if (noun.getNounType() == PixelDataType.FRAME) {
          ITableDataFrame frame = (ITableDataFrame) noun.getValue();
          FrameCacheHelper existingFrameObject = InsightUtility.findSameFrame(this.frames, frame);
          if (existingFrameObject != null) {
            existingFrameObject.addAlias(k);
          } else {
            FrameCacheHelper fObj = new FrameCacheHelper(frame);
            fObj.addAlias(k);
            this.frames.add(fObj);
          }
        } else {
          if (this.keysToIgnore.contains(k)) {
            continue;
          }
          // normal noun
          out.name(k);
          NounMetadataAdapter adapter = new NounMetadataAdapter();
          adapter.write(out, noun);
        }
      }

      // loop through the frames
      // if they contain the keys to ignore
      // remove them
      Iterator<FrameCacheHelper> iterator = this.frames.iterator();
      while (iterator.hasNext()) {
        FrameCacheHelper fObj = iterator.next();
        List<String> alias = fObj.getAlias();
        for (String a : alias) {
          if (this.keysToIgnore.contains(a)) {
            iterator.remove();
          }
        }
      }
    } else {
      for (String k : keys) {
        if (this.keysToIgnore.contains(k)) {
          continue;
        }
        // ignore anything that is a task or a frame
        NounMetadata noun = value.get(k);
        if (noun.getNounType() != PixelDataType.TASK && noun.getNounType() != PixelDataType.FRAME) {
          // normal noun
          out.name(k);
          NounMetadataAdapter adapter = new NounMetadataAdapter();
          adapter.write(out, noun);
        }
      }
    }

    out.endObject();
  }

  @Override
  public VarStore read(JsonReader in) throws IOException {
    VarStore store = new VarStore();

    in.beginObject();
    while (in.hasNext()) {
      String key = in.nextName();
      NounMetadataAdapter adapter = new NounMetadataAdapter();
      NounMetadata noun = adapter.read(in);
      store.put(key, noun);
    }
    in.endObject();

    return store;
  }

  public void setKeysToIgnore(Set<String> keysToIgnore) {
    this.keysToIgnore = keysToIgnore;
  }

  public void setCollectFrames(boolean collectFrames) {
    this.collectFrames = collectFrames;
    this.frames = new Vector<FrameCacheHelper>();
  }

  public List<FrameCacheHelper> getFrames() {
    return this.frames;
  }
}
