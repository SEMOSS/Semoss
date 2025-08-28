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
package prerna.engine.impl.vector;

import java.util.List;

public class VectorDatabaseCSVRow {

  private List<? extends Number> embeddings =
      null; // This could be a placeholder or identifier for actual embeddings
  private String source;
  private String modality;
  private String divider;
  private String part;
  private Integer tokens;
  private String content;

  // TODO: revisit how this is stored in db
  private String keywords = "";

  public VectorDatabaseCSVRow(
      String source, String modality, String divider, String part, Number tokens, String content) {
    // Initially, embedding might not be set
    this.source = source;
    this.modality = modality;
    this.divider = divider;
    this.part = part;
    this.tokens = tokens.intValue();
    this.content = content;
  }

  // Method to update the embeddings for a row
  public void setEmbeddings(List<? extends Number> list) {
    this.embeddings = list;
  }

  public List<? extends Number> getEmbeddings() {
    return this.embeddings;
  }

  public String getSource() {
    return this.source;
  }

  public String getModality() {
    return this.modality;
  }

  public String getDivider() {
    return this.divider;
  }

  public String getPart() {
    return this.part;
  }

  public Integer getTokens() {
    return this.tokens;
  }

  public String getContent() {
    return this.content;
  }

  public void setKeywords(String keywords) {
    this.keywords = keywords;
  }

  public String getKeywords() {
    return this.keywords;
  }
}
