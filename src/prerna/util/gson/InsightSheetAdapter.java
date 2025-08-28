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
import prerna.om.InsightSheet;

public class InsightSheetAdapter extends AbstractSemossTypeAdapter<InsightSheet> {

  @Override
  public void write(JsonWriter out, InsightSheet value) throws IOException {
    // this is just a simple key/value
    String sheetId = value.getSheetId();
    String sheetLabel = value.getSheetLabel();
    String backgroundColor = value.getBackgroundColor();
    Boolean hideHeaders = value.getHideHeaders();
    Boolean hideBorders = value.getHideBorders();
    int borderSize = value.getBorderSize();
    String height = value.getHeight();
    String width = value.getWidth();
    int gutterSize = value.getGutterSize();

    out.beginObject();
    out.name("sheetId").value(sheetId);
    if (sheetLabel == null) {
      out.name("sheetLabel").nullValue();
    } else {
      out.name("sheetLabel").value(sheetLabel);
    }
    if (backgroundColor == null) {
      out.name("backgroundColor").nullValue();
    } else {
      out.name("backgroundColor").value(backgroundColor);
    }
    if (hideHeaders == null) {
      out.name("hideHeaders").nullValue();
    } else {
      out.name("hideHeaders").value(hideHeaders);
    }
    if (hideBorders == null) {
      out.name("hideBorders").nullValue();
    } else {
      out.name("hideBorders").value(hideBorders);
    }
    if (height == null) {
      out.name("height").nullValue();
    } else {
      out.name("height").value(height);
    }
    if (width == null) {
      out.name("width").nullValue();
    } else {
      out.name("width").value(width);
    }
    // border size has default of 2
    out.name("borderSize").value(borderSize);
    // gutter size has default of 2
    out.name("gutterSize").value(gutterSize);

    out.endObject();
  }

  @Override
  public InsightSheet read(JsonReader in) throws IOException {
    String sheetId = null;
    String sheetLabel = null;
    String backgroundColor = null;
    Boolean hideHeaders = null;
    Boolean hideBorders = null;
    Integer borderSize = null;
    String height = null;
    String width = null;
    Integer gutterSize = null;

    in.beginObject();
    while (in.hasNext()) {
      String key = in.nextName();
      if (in.peek() == JsonToken.NULL) {
        in.nextNull();
        continue;
      }

      if (key.equals("sheetId")) {
        sheetId = in.nextString();
      } else if (key.equals("sheetLabel")) {
        sheetLabel = in.nextString();
      } else if (key.equals("backgroundColor")) {
        backgroundColor = in.nextString();
      } else if (key.equals("hideHeaders")) {
        hideHeaders = in.nextBoolean();
      } else if (key.equals("hideBorders")) {
        hideBorders = in.nextBoolean();
      } else if (key.equals("borderSize")) {
        borderSize = in.nextInt();
      } else if (key.equals("height")) {
        height = in.nextString();
      } else if (key.equals("width")) {
        width = in.nextString();
      } else if (key.equals("gutterSize")) {
        gutterSize = in.nextInt();
      }
    }
    in.endObject();

    InsightSheet sheet = new InsightSheet(sheetId);
    sheet.setSheetLabel(sheetLabel);
    sheet.setBackgroundColor(backgroundColor);
    sheet.setHideHeaders(hideHeaders);
    sheet.setHideBorders(hideBorders);
    sheet.setHeight(height);
    sheet.setWidth(width);
    if (borderSize != null) {
      sheet.setBorderSize(borderSize);
    }
    if (gutterSize != null) {
      sheet.setGutterSize(gutterSize);
    }
    return sheet;
  }
}
