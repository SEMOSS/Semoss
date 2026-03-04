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
package prerna.reactor.frame.r;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.AddHeaderNounMetadata;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ConcatenateReactor extends AbstractRFrameReactor {

	public ConcatenateReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DELIMITER.getKey(), ReactorKeysEnum.NEW_COLUMN.getKey(),
				ReactorKeysEnum.VALUES.getKey() };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		RDataTable rFrame = (RDataTable) getFrame();
		String frameName = rFrame.getName();
		String delim = this.keyValue.get(ReactorKeysEnum.DELIMITER.getKey());
		String newColName = this.keyValue.get(ReactorKeysEnum.NEW_COLUMN.getKey());
		if (newColName == null || newColName.isEmpty()) {
			throw new IllegalArgumentException("Need to define the new column name");
		}
		// clean the column name to ensure that it is valid
		newColName = getCleanNewColName(rFrame, newColName);

		GenRowStruct val_grs = this.store.getGenRowStruct(this.keysToGet[2]);
		StringBuilder rsb = new StringBuilder();
		for (int i = 0; i < val_grs.size(); i++) {
			NounMetadata noun = val_grs.getNoun(i);
			Object val = noun.getValue();
			if (noun.getNounType().equals(PixelDataType.COLUMN)) {
				rsb.append(frameName + "$" + val);
			} else {
				rsb.append(val);
			}
			if ((i + 1) != val_grs.size()) {
				rsb.append(", ");
			}
		}
		// Execute RSyntax
		// paste(val,val2,..., sep="delim")
		String rScript = frameName + "$" + newColName + " <- paste(" + rsb.toString() + ", sep=\"" + delim + "\")";
		this.rJavaTranslator.executeEmptyR(rScript);
		this.addExecutedCode(rScript);

		// check if new column exists
		String colExistsScript = "\"" + newColName + "\" %in% colnames(" + frameName + ")";
		boolean colExists = this.rJavaTranslator.getBoolean(colExistsScript);
		if (!colExists) {
			NounMetadata error = NounMetadata.getErrorNounMessage("Unable to Concatenate values");
			SemossPixelException exception = new SemossPixelException(error);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		// update the metadata to include this new column
		OwlTemporalEngineMeta metaData = this.getFrame().getMetaData();
		metaData.addProperty(frameName, frameName + "__" + newColName);
		metaData.setAliasToProperty(frameName + "__" + newColName, newColName);
		metaData.setDataTypeToProperty(frameName + "__" + newColName, SemossDataType.STRING.toString());
		rFrame.syncHeaders();

		NounMetadata retNoun = new NounMetadata(rFrame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE,
				PixelOperationType.FRAME_DATA_CHANGE);
		retNoun.addAdditionalReturn(new AddHeaderNounMetadata(newColName));
		retNoun.addAdditionalReturn(
				NounMetadata.getSuccessNounMessage("Successfully Concatenated values into " + newColName));
		return retNoun;
	}

}
