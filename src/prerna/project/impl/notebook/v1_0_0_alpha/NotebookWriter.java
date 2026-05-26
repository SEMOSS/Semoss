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
package prerna.project.impl.notebook.v1_0_0_alpha;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;

import prerna.project.impl.notebook.INotebookBuilder;
import prerna.util.Utility;

public class NotebookWriter implements INotebookBuilder {

	private static final Logger classLogger = LogManager.getLogger(NotebookWriter.class);

	private JsonObject blocksFileJson = null;

	@Override
	public JsonElement getBlocksFileJson() {
		return this.blocksFileJson;
	}

	@Override
	public void setBlocksFileJson(JsonElement blocksFileJson) {
		try {
			this.blocksFileJson = blocksFileJson.getAsJsonObject();
		} catch (IllegalStateException e) {
			classLogger.error("Failed to parse blocks file json as a JsonObject: {}", e.getMessage(), e);
			throw new IllegalArgumentException("The json is not of the valid format for this version.", e);
		}
	}

	@Override
	public List<File> createNotebooks(File writeDir) {
		List<File> notebookList = new ArrayList<>();
		Gson gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();

		try {
			FileUtils.cleanDirectory(writeDir);

			// prefer the "notebooks" key; fall back to the legacy "queries" key when
			// "notebooks" is missing or empty so previously-saved projects still export
			JsonObject blocksNotebookMap = blocksFileJson.getAsJsonObject("notebooks");
			if (blocksNotebookMap == null || blocksNotebookMap.size() == 0) {
				blocksNotebookMap = blocksFileJson.getAsJsonObject("queries");
			}
			if (blocksNotebookMap == null) {
				classLogger.warn("No 'notebooks' or 'queries' present in the blocks json; nothing to write");
				return notebookList;
			}
			for (String notebookName : blocksNotebookMap.keySet()) {
				// these are from the blocks json
				JsonObject blocksNotebook = blocksNotebookMap.getAsJsonObject(notebookName);
				List<JsonElement> blocksCells = blocksNotebook.getAsJsonArray("cells").asList();

				// we now need to move the information from the blocks json
				// into the notebook we are writing
				File writeNotebook = new File(
						Utility.normalizePath(writeDir.getAbsolutePath() + "/" + notebookName + ".ipynb"));

				JsonArray cellsArray = new JsonArray();
				for (JsonElement blocksCell : blocksCells) {
					JsonObject blocksParam = blocksCell.getAsJsonObject().getAsJsonObject("parameters");

					String blockType = blocksParam.get("type").getAsString();
					String blockValue = blocksParam.get("code").getAsString();

					String cell_type = null;
					String id = Utility.getRandomString(8);
					String source = blockValue;

					if (blockType.equalsIgnoreCase("py") || blockType.equalsIgnoreCase("r")) {
						cell_type = "code";
					} else if (blockType.equalsIgnoreCase("markdown")) {
						cell_type = "raw";
					} else {
						cell_type = "markdown";
					}

					JsonObject cellObject = new JsonObject();
					cellObject.addProperty("cell_type", cell_type);
					cellObject.addProperty("id", id);
					// will add empty metadata for now
					cellObject.add("metadata", new JsonObject());
					JsonArray sourceEle = new JsonArray();
					sourceEle.add(source);
					cellObject.add("source", sourceEle);

					// now add this to the cells array
					cellsArray.add(cellObject);
				}

				JsonObject writeJson = new JsonObject();
				writeJson.add("cells", cellsArray);

				// write to the notebook file
				try (JsonWriter writer = gson.newJsonWriter(new FileWriter(writeNotebook))) {
					gson.toJson(writeJson, writer);
				}
				// add to list of notebooks
				notebookList.add(writeNotebook);
			}
		} catch (IOException e) {
			classLogger.error("Failed to create notebook files in directory '{}': {}", writeDir.getAbsolutePath(),
					e.getMessage(), e);
			throw new IllegalArgumentException("Error occurred trying to create the notebook for this app");
		}

		return notebookList;
	}

}
