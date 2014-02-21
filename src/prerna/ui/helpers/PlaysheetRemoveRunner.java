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
package prerna.ui.helpers;

import java.util.ArrayList;
import java.util.Vector;

import javax.swing.JInternalFrame;
import javax.swing.JScrollPane;
import javax.swing.JTable;

import prerna.ui.components.GridFilterData;
import prerna.ui.components.GridTableModel;
import prerna.ui.components.NewScrollBarUI;
import prerna.ui.components.playsheets.GraphPlaySheet;

/**
 * This class helps with running the remove view method for a playsheet.
 */
public class PlaysheetRemoveRunner implements Runnable{

	GraphPlaySheet playSheet = null;
	
	/**
	 * Constructor for PlaysheetRemoveRunner.
	 * @param playSheet GraphPlaySheet
	 */
	public PlaysheetRemoveRunner(GraphPlaySheet playSheet)
	{
		this.playSheet = playSheet;
	}

	/**
	 * Method run.  Instantiates a graph play sheet on the local playsheet, and runs createRemoveGrid.
	 */
	@Override
	public void run() {
		playSheet.removeView();
		GraphPlaySheet gPlaySheet = (GraphPlaySheet) playSheet;
		createRemoveGrid(gPlaySheet);
	}
	
	/**
	 * Method createRemoveGrid.  Removes the edges from grid filter data.
	 * @param gPlaySheet GraphPlaySheet - the playsheet to run.
	 */
	public void createRemoveGrid(GraphPlaySheet gPlaySheet) {
		
		GridFilterData gfd = new GridFilterData();
		JInternalFrame techMatSheet = new JInternalFrame();
		String[] colNames = new String[1];
		colNames[0] = "Edges Removed";
		gfd.setColumnNames(colNames);
		ArrayList <Object []> list = new ArrayList();
//		Vector edgeV = gPlaySheet.edgeVector;
//		for(int i = 0; i < edgeV.size(); i++){
//			String [] strArray = new String[1];
//			strArray[0]=(String) edgeV.get(i);
//			list.add(i, strArray);
//		}
		gfd.setDataList(list);
		JTable table = new JTable();
		GridTableModel model = new GridTableModel(gfd);
		table.setModel(model);
		JScrollPane scrollPane = new JScrollPane(table);
		scrollPane.getVerticalScrollBar().setUI(new NewScrollBarUI());
		scrollPane.setAutoscrolls(true);
		techMatSheet.setContentPane(scrollPane);
		
		gPlaySheet.jTab.add("Edges Removed", techMatSheet);
		techMatSheet.setClosable(true);
		techMatSheet.setMaximizable(true);
		techMatSheet.setIconifiable(true);
		techMatSheet.setTitle("Edges Removal");
		techMatSheet.pack();
		techMatSheet.setVisible(true);

	}
	
	/**
	 * Method setPlaySheet. Sets the local playsheet to the parameter.
	 * @param playSheet GraphPlaySheet
	 */
	public void setPlaySheet(GraphPlaySheet playSheet)
	{
		this.playSheet = playSheet;
	}

}
