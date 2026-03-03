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

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.py.PandasFrame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ToPercentReactor extends AbstractPyFrameReactor {

	private static final String BY100 = "by100";
	private static final String SIG_DIGITS = "sigDigits";

	public ToPercentReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), SIG_DIGITS, BY100,
				ReactorKeysEnum.NEW_COLUMN.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();

		// get the wrapper name
		String wrapperFrameName = frame.getWrapperName();

		// get inputs
		String srcCol = this.keyValue.get(ReactorKeysEnum.COLUMN.getKey());
		int sigDigits = getValue(SIG_DIGITS);
		boolean by100 = getBoolean(BY100, false);
		String newColName = this.keyValue.get(ReactorKeysEnum.NEW_COLUMN.getKey());
		String script = null;
		// create script
		if (newColName != null && !newColName.equals("") && !newColName.equals("null")) {
			script = wrapperFrameName + ".to_pct('" + srcCol + "', '" + srcCol + "', " + sigDigits + ", ";
		} else {
			script = wrapperFrameName + ".to_pct('" + srcCol + "', '" + newColName + "', " + sigDigits + ", ";
		}
		if (by100) {
			script += "True)";
		} else {
			script += "False)";
		}

		String by100v = by100 ? "True" : "False";

		if (sigDigits < 0) {
			throw new IllegalArgumentException("Significant digits must be greater than or equal to zero.");
		}

		script = wrapperFrameName + ".cache['data']['" + newColName + "'] = " + wrapperFrameName + ".cache['data']['"
				+ srcCol + "'].apply(lambda row: " + "str(round(row, " + sigDigits + ") * 100) + '%' if " + by100v
				+ " else " + "str(round(row, " + sigDigits + ") * 1) + '%' )" + ".replace(\'nan%\','null')"; // this

		// run script
		// converting to lambda
		// mv['add'] = mv.apply(lambda x: clean.PyFrame.to_pct_l(x['MovieBudget'], 2, 1)
		// , axis=1)
		insight.getPyTranslator().runEmptyPy(script);
		this.addExecutedCode(script);

		// update meta data
		OwlTemporalEngineMeta metaData = frame.getMetaData();
		String frameName = frame.getName();
		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);

		if (newColName != null && !newColName.equals("") && !newColName.equals("null")) {
			retNoun.addAdditionalOpTypes(PixelOperationType.FRAME_HEADERS_CHANGE);
			String addedColumnDataType = SemossDataType.STRING.toString();
			metaData.addProperty(frameName, frameName + "__" + newColName);
			metaData.setAliasToProperty(frameName + "__" + newColName, newColName);
			metaData.setDataTypeToProperty(frameName + "__" + newColName, addedColumnDataType);
			metaData.setDerivedToProperty(frameName + "__" + newColName, true);
			frame.syncHeaders();
		} else {
			metaData.modifyDataTypeToProperty(frameName + "__" + srcCol, frameName, SemossDataType.STRING.toString());
		}

		// return the output
		return retNoun;
	}

	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////

	private int getValue(String key) {
		GenRowStruct grs = this.store.getGenRowStruct(key);
		NounMetadata noun = grs.getNoun(0);

		if (noun.getNounType() == PixelDataType.CONST_INT) {
			return (int) grs.get(0);
		} else {
			throw new IllegalArgumentException(
					"Input of " + grs.get(0) + " is invalid. Significant digits must be an integer value.");
		}
	}
}
