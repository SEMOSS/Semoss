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
package prerna.ui.components.playsheets;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLEnum.PKQLReactor;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;

/**
 * The DashboardPlaySheet class is used to send individual dashboard insights back to the front-end and does not process any query.
 */

public class MashupPlaySheet extends AbstractPlaySheet implements IDataMaker {
	static final Logger logger = LogManager.getLogger(MashupPlaySheet.class.getName());

//	@Override
//	public Object getData() {
//		Hashtable returnHash = (Hashtable) super.getData();
//		returnHash.put("specificData", query);
//		
//		logger.info("Dashboard data: " + query);
//		return returnHash;
//	}
	
	@Override
	public void refineView() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void overlayView() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void createData() {
		// TODO Auto-generated method stub

	}

	@Override
	public void runAnalytics() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Hashtable<String, String> getDataTableAlign() {
		// No table to align to for mash ups at this point
		return null;
	}

	@Override
	public void setQuery(String query) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void processQueryData() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setDataMaker(IDataMaker data) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public IDataMaker getDataMaker() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void processDataMakerComponent(DataMakerComponent component) {
		this.query = component.getQuery();
	}

	@Override
	public Map getDataMakerOutput(String... selectors) {
//		Hashtable returnHash = (Hashtable) super.getData();
		Hashtable returnHash = new Hashtable();
		returnHash.put("specificData", query);
		logger.info("Dashboard data: " + query);
		return returnHash;
	}

	@Override
	public IDataMaker getDefaultDataMaker() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void processPreTransformations(DataMakerComponent dmc,
			List<ISEMOSSTransformation> transforms) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void processPostTransformations(DataMakerComponent dmc,
			List<ISEMOSSTransformation> transforms, IDataMaker... dataFrame) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, String> getScriptReactors() {
		Map<String, String> reactorNames = new HashMap<String, String>();
		reactorNames.put(PKQLEnum.EXPR_TERM, "prerna.sablecc.ExprReactor");
		reactorNames.put(PKQLEnum.EXPR_SCRIPT, "prerna.sablecc.ExprReactor");
		reactorNames.put(PKQLReactor.MATH_FUN.toString(), "prerna.sablecc.MathReactor");
//		reactorNames.put(PKQLEnum.COL_CSV, "prerna.sablecc.TinkerColAddReactor");
		reactorNames.put(PKQLEnum.ROW_CSV, "prerna.sablecc.RowCsvReactor");
		reactorNames.put(PKQLEnum.API, "prerna.sablecc.ApiReactor");
//		reactorNames.put(PKQLEnum.PASTED_DATA, "prerna.sablecc.PastedDataReactor");
		reactorNames.put(PKQLEnum.WHERE, "prerna.sablecc.ColWhereReactor");
		reactorNames.put(PKQLEnum.REL_DEF, "prerna.sablecc.RelReactor");
//		reactorNames.put(PKQLEnum.COL_ADD, "prerna.sablecc.ColAddReactor");
//		reactorNames.put(PKQLEnum.IMPORT_DATA, "prerna.sablecc.GDMImportDataReactor");
//		reactorNames.put(PKQLEnum.REMOVE_DATA, "prerna.sablecc.RemoveDataReactor");
//		reactorNames.put(PKQLEnum.FILTER_DATA, "prerna.sablecc.ColFilterReactor");
		reactorNames.put(PKQLEnum.VIZ, "prerna.sablecc.VizReactor");
//		reactorNames.put(PKQLEnum.UNFILTER_DATA, "prerna.sablecc.ColUnfilterReactor");
//		reactorNames.put(PKQLEnum.DATA_FRAME, "prerna.sablecc.DataFrameReactor");
//		switch(reactorType) {
//			case IMPORT_DATA : return new GDMImportDataReactor();
//			case COL_ADD : return new ColAddReactor();
//		}
		
		return reactorNames;
	}

	@Override
	public void setUserId(String userId) {
		// TODO Auto-generated method stub
		
	}

	
	@Override
	public String getUserId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getDataMakerName() {
		return this.getClass().getName();
	}

	@Override
	public void updateDataId() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getDataId() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void resetDataId() {
		// TODO Auto-generated method stub
		
	}
}
