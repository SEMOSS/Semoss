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
package prerna.reactor.frame.py;

import prerna.ds.py.PandasFrame;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.ds.py.PyTranslator;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class MatchColumnValuesReactor extends AbstractPyFrameReactor {

	public MatchColumnValuesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String column = this.keyValue.get(this.keysToGet[0]);

		// get single column input
		PandasFrame frame = (PandasFrame) getFrame();
		String wrapperName = frame.getWrapperName();

		String matchesTable = Utility.getRandomString(8);
		String script = matchesTable + " = " + wrapperName + ".self_match('" + column + "')";
		insight.getPyTranslator().runEmptyPy(script);
		this.addExecutedCode(script);

		PyTranslator pyTranslator = this.insight.getPyTranslator();
		PandasFrame returnTable = new PandasFrame(matchesTable, pyTranslator);
		pyTranslator.runEmptyPy(PandasSyntaxHelper.makeWrapper(returnTable.getWrapperName(), matchesTable));
		returnTable = (PandasFrame) recreateMetadata(returnTable, false);

		NounMetadata retNoun = new NounMetadata(returnTable, PixelDataType.FRAME);

		// get count of exact matches
		Number exactMatchCount = (Number) returnTable
				.runScript("len(" + matchesTable + "[" + matchesTable + "['distance'] == 100])");
		if (exactMatchCount != null) {
			retNoun.addAdditionalReturn(new NounMetadata(exactMatchCount.longValue(), PixelDataType.CONST_INT));
		} else {
			throw new IllegalArgumentException("No matches found.");
		}

		this.insight.getVarStore().put(matchesTable, retNoun);
		return retNoun;
	}
}
