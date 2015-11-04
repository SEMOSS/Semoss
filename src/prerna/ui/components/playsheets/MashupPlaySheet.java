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

import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.ui.components.playsheets.datamakers.ISEMOSSAction;
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
	public Map getDataMakerOutput() {
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
	public List<Object> processActions(DataMakerComponent dmc, List<ISEMOSSAction> actions, IDataMaker... dataMaker) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Object> getActionOutput() {
		// TODO Auto-generated method stub
		return null;
	}

}
