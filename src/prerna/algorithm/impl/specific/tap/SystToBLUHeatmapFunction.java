/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.algorithm.impl.specific.tap;



import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.beans.PropertyVetoException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import javax.swing.JDesktopPane;

import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.teamdev.jxbrowser.chromium.LoggerProvider;

import prerna.algorithm.api.IAlgorithm;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.GridScrollPane;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.ui.components.specific.tap.SimilarityFunctions;
import prerna.ui.components.specific.tap.SimilarityHeatMapSheet;
import prerna.ui.components.specific.tap.SysToBLUDataGapsPlaySheet;
import prerna.ui.components.specific.tap.BLUSysComparison;
import prerna.ui.main.listener.specific.tap.SimilarityBarChartBrowserFunction;
import prerna.ui.main.listener.specific.tap.SimilarityRefreshBrowserFunction;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.QuestionPlaySheetStore;

/**
 * This class is used to process through two variables to identify relationships.
 */
public class SystToBLUHeatmapFunction implements IAlgorithm {	
	Logger logger = Logger.getLogger(getClass());
	SysToBLUDataGapsPlaySheet playSheet;
	IEngine engine;
	String[] names; 

	ArrayList<String> rowNames = new ArrayList<String>();
	ArrayList<String> colNames = new ArrayList<String>();

	
	public void updateHeatMap()
	{
		Vector <String> rowNamesAsVector = new Vector<String>(rowNames);
		Collections.sort(rowNamesAsVector);
		rowNames = new ArrayList<String>(rowNamesAsVector);

		Vector <String> colNamesAsVector = new Vector<String>(colNames);
		Collections.sort(colNamesAsVector);
		colNames = new ArrayList<String>(colNamesAsVector);
		
		BLUSysComparison bsc = null;

		try {
			bsc = (BLUSysComparison)Class.forName("prerna.ui.components.specific.tap.BLUSysComparison").getConstructor(null).newInstance(null);
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		playSheet.HeatPanel.removeAll();
		playSheet.HeatPanel.setLayout(new GridLayout(1, 0, 0, 0));
//		JDesktopPane jdp = new JDesktopPane();
//		playSheet.HeatPanel.add(jdp);
		
		playSheet.HeatPanel.add(bsc);
		
		bsc.setRDFEngine((IEngine)DIHelper.getInstance().getLocalProp("HR_Core"));
		bsc.setPlaySheet(playSheet);
//		bsc.setJDesktopPane(jdp);
//		bsc.setQuestionID("Graph Analysis PlaySheet");
//		bsc.setTitle("Graph Analysis");

		bsc.createData(rowNames, colNames);
		bsc.runAnalytics();
		bsc.createView();
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

	public void setDataList(ArrayList<String> dataList)
	{
		this.rowNames = dataList;
	}

	public void setSysList(ArrayList<String> sysList)
	{
		this.colNames = sysList;
	}

}
