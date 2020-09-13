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
package prerna.algorithm.impl.specific.tap;



import java.awt.GridLayout;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.IAlgorithm;
import prerna.engine.api.IEngine;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.specific.tap.BLUSysComparison;
import prerna.ui.components.specific.tap.SysToBLUDataGapsPlaySheet;
import prerna.util.DIHelper;

/**
 * This class is used to process through two variables to identify relationships.
 */
public class SystToBLUHeatmapFunction implements IAlgorithm {	
	static final Logger logger = LogManager.getLogger(SystToBLUHeatmapFunction.class);

	private static final String STACKTRACE = "StackTrace: ";

	SysToBLUDataGapsPlaySheet playSheet;
	IEngine engine;
	String[] names; 

	List<String> rowNames = new ArrayList<>();
	List<String> colNames = new ArrayList<>();

	
	public void updateHeatMap()
	{
		Vector<String> rowNamesAsVector = new Vector<>(rowNames);
		Collections.sort(rowNamesAsVector);
		rowNames = new ArrayList<>(rowNamesAsVector);

		Vector<String> colNamesAsVector = new Vector<>(colNames);
		Collections.sort(colNamesAsVector);
		colNames = new ArrayList<>(colNamesAsVector);
		
		BLUSysComparison bsc = null;

		try {
			bsc = (BLUSysComparison) Class.forName("prerna.ui.components.specific.tap.BLUSysComparison").getConstructor(null).newInstance(null);
		} catch (InstantiationException ie) {
			logger.error(STACKTRACE, ie);
		} catch (IllegalAccessException iae) {
			logger.error(STACKTRACE, iae);
		} catch (IllegalArgumentException iar) {
			logger.error(STACKTRACE, iar);
		} catch (InvocationTargetException ite) {
			logger.error(STACKTRACE, ite);
		} catch (NoSuchMethodException nsme) {
			logger.error(STACKTRACE, nsme);
		} catch (SecurityException se) {
			logger.error(STACKTRACE, se);
		} catch (ClassNotFoundException cnf) {
			logger.error(STACKTRACE, cnf);
		}
		playSheet.HeatPanel.removeAll();
		playSheet.HeatPanel.setLayout(new GridLayout(1, 0, 0, 0));
		playSheet.HeatPanel.add(bsc);

		if (bsc != null) {
			bsc.setRDFEngine((IEngine)DIHelper.getInstance().getLocalProp("TAP_Core_Data"));
			bsc.setPlaySheet(playSheet);
			bsc.createData(rowNames, colNames);
			bsc.runAnalytics();
			bsc.createView();
		}
	}
		
	/**
	 * Casts a given playsheet as a heat map playsheet.
	 * @param playSheet 	Playsheet to be cast.
	 */

	public void setPlaySheet(IPlaySheet playSheet) {
		this.playSheet = (SysToBLUDataGapsPlaySheet) playSheet;
	}

	@Override
	public String[] getVariables() {
		return null;
	}

	@Override
	public void execute() {
		updateHeatMap();
	}

	@Override
	public String getAlgoName() {
		return null;
	}

	public void setRDFEngine(IEngine engine) {
		this.engine = engine;	
	}

	public void setDataList(List<String> dataList)
	{
		this.rowNames = dataList;
	}

	public void setSysList(List<String> sysList)
	{
		this.colNames = sysList;
	}

}
