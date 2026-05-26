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
package prerna.reactor.algorithms;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class RAlgReactor extends AbstractRFrameReactor {

	private static final String CLASS_NAME = RAlgReactor.class.getName();

	public RAlgReactor() {
		this.keysToGet = new String[] { "filename", "function" };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		init();

		Map<String, String> fInputs = getFunctionInputs();
		String fileName = fInputs.get(this.keysToGet[0]);
		String functionName = fInputs.get(this.keysToGet[1]);

		String fileLoc = Utility.getBaseFolder();
		fileLoc = fileLoc.replace("\\", "/") + "/R/UserScripts/" + fileName;

		// loop through and generate the syntax
		StringBuilder script = new StringBuilder();
		script.append("source(\"").append(fileLoc).append("\");");
		script.append(functionName).append("(");
		int counter = 0;
		for (String key : fInputs.keySet()) {
			if (key.equals(this.keysToGet[0]) || key.equalsIgnoreCase(this.keysToGet[1])) {
				continue;
			}

			String inputValue = fInputs.get(key);
			if (counter == 0) {
				script.append(key).append("=").append(inputValue);
			} else {
				script.append(",").append(key).append("=").append(inputValue);
			}
			counter++;
		}
		script.append(");");

		String scriptStr = script.toString();
		logger.info("Running script : " + Utility.cleanLogString(scriptStr));
		this.rJavaTranslator.runR(scriptStr);

		return new NounMetadata(scriptStr, PixelDataType.CONST_STRING);
	}

	/**
	 * Merging all the inputs into R specific syntax Will generate appropriate
	 * vector syntax vs. scalar syntax for inputs NOTE ::: Cannot have the variable
	 * name "all" since its already used by the reactor
	 * 
	 * @return
	 */
	private Map<String, String> getFunctionInputs() {
		Map<String, String> inputs = new HashMap<String, String>();

		Set<String> keys = this.store.getNounKeys();
		for (String key : keys) {
			if (key.equals("all")) {
				// ignore the all key
				continue;
			} else if (key.equals(this.keysToGet[0]) || key.equals(this.keysToGet[1])) {
				// this is a string but we do not want quotes around it
				inputs.put(key, this.store.getGenRowStruct(key).get(0).toString());
			} else {
				// other input to process
				GenRowStruct grs = this.store.getGenRowStruct(key);
				int size = grs.size();
				if (size == 1) {
					NounMetadata noun = grs.getNoun(0);
					if (noun.getNounType() == PixelDataType.CONST_STRING) {
						// we have a string
						// wrap it in quotes
						inputs.put(key, "\"" + noun.getValue() + "\"");

					} else if (noun.getNounType() == PixelDataType.CONST_INT
							|| noun.getNounType() == PixelDataType.CONST_DECIMAL) {
						// we have a number
						// just stringify it without adding quotes
						inputs.put(key, noun.getValue() + "");
					}
				} else {
					// right now, assume it is a string array
					StringBuilder script = new StringBuilder("c(");
					NounMetadata noun = grs.getNoun(0);
					script.append("\"" + noun.getValue() + "\"");
					for (int i = 1; i < size; i++) {
						noun = grs.getNoun(1);
						script.append(", \"" + noun.getValue() + "\"");
					}
					script.append(")");
					// add the column vec
					inputs.put(key, script.toString());
				}
			}
		}

		return inputs;
	}
}
