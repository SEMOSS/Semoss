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
package prerna.reactor.imports;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.om.Insight;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SQLQueryUtils;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SubqueryReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(SubqueryReactor.class);

	public SubqueryReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FRAME.getKey(), ReactorKeysEnum.QUERY_STRUCT.getKey() };
	}

	@Override
	public NounMetadata execute() {
		ITableDataFrame curFrame = this.insight.getCurFrame();
		SelectQueryStruct qs = getQueryStruct();
		if (qs != null) {
			AbstractQueryStruct.QUERY_STRUCT_TYPE type = qs.getQsType();
			if ((type == AbstractQueryStruct.QUERY_STRUCT_TYPE.FRAME
					|| type == AbstractQueryStruct.QUERY_STRUCT_TYPE.RAW_FRAME_QUERY) && qs.getFrame() == null) {
				qs.setFrame(curFrame);
			}
		}
		ITableDataFrame mergeFrame = null;

		// are they both native
		if (curFrame instanceof NativeFrame && curFrame != null && qs != null) {
			try {
				qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, qs.getFrame().getMetaData());
				mergeFrame = SQLQueryUtils.subQuery(((NativeFrame) curFrame).getQueryStruct(), qs);
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				classLogger.error("Failed to perform the subquery merge on the native frame", e1);
			}
		}

		// clear cached info after merge
		curFrame.clearCachedMetrics();
		curFrame.clearQueryCache();

		NounMetadata noun = new NounMetadata(mergeFrame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE,
				PixelOperationType.FRAME_HEADERS_CHANGE);
		// in case we generated a new frame
		// update existing references
		if (mergeFrame != curFrame) {
			if (curFrame.getName() != null) {
				this.insight.getVarStore().put(curFrame.getName(), noun);
			}
			if (curFrame == this.insight.getVarStore().get(Insight.CUR_FRAME_KEY).getValue()) {
				this.insight.setDataMaker(mergeFrame);
			}
		}

		return noun;
	}

	///////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////

	protected SelectQueryStruct getQueryStruct() {
		SelectQueryStruct queryStruct = null;

		GenRowStruct grs = this.store.getGenRowStruct(this.keysToGet[1]);
		if (grs != null) {
			NounMetadata object = grs.getNoun(0);
			return (SelectQueryStruct) object.getValue();
		}

		grs = this.store.getGenRowStruct(PixelDataType.QUERY_STRUCT.toString());
		if (grs != null) {
			NounMetadata object = grs.getNoun(0);
			return (SelectQueryStruct) object.getValue();
		}

		return queryStruct;
	}

}