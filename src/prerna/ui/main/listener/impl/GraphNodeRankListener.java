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
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import javax.swing.JInternalFrame;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.om.SEMOSSVertex;
import prerna.ui.components.GridFilterData;
import prerna.ui.components.GridScrollPane;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.QuestionPlaySheetStore;
import edu.uci.ics.jung.algorithms.scoring.PageRank;

/**
 * Controls the running of the node rank algorithm
 */
public class GraphNodeRankListener implements ActionListener {

	GraphPlaySheet ps = null;
	SEMOSSVertex [] vertices = null;
	
	static final Logger logger = LogManager.getLogger(GraphNodeRankListener.class.getName());
	/**
	 * Method setPlaysheet.  Sets the playsheet that the listener will access.
	 * @param ps GraphPlaySheet
	 */
	public void setPlaysheet(GraphPlaySheet ps)
	{
		this.ps = ps;
	}
		
	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param e ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		GraphPlaySheet playSheet = (GraphPlaySheet) QuestionPlaySheetStore.getInstance().getActiveSheet();
		
		//set up page rank
		double alpha = 0.15;
		double tolerance = 0.001;
		int maxIterations = 100;
		PageRank<SEMOSSVertex, Integer> ranker = new PageRank<SEMOSSVertex, Integer>(playSheet.forest, alpha);
		
		ranker.setTolerance(tolerance) ;
		ranker.setMaxIterations(maxIterations);
		ranker.evaluate();
		
		Collection<String> col =  playSheet.forest.getVertices();
		
		Iterator it = col.iterator();
		GridFilterData gfd = new GridFilterData();
		JInternalFrame nodeRankSheet = new JInternalFrame();
		String[] colNames = new String[3];
		colNames[0] = "Vertex Name";
		colNames[1] = "Vertex Type";
		colNames[2] = "Page Rank Score";
		gfd.setColumnNames(colNames);
		ArrayList <Object []> list = new ArrayList();
		ArrayList numList = new ArrayList();
		int count = 0;
		
		//process through graph and list out all nodes, type, pagerank
		while (it.hasNext()) {
			SEMOSSVertex v= (SEMOSSVertex) it.next();
			String url = v.getURI();
			String[] urlSplit = url.split("/");
			double r = ranker.getVertexScore(v);
			
			String [] scores = new String[colNames.length];
			scores[0] = urlSplit[urlSplit.length-1];
			scores[1] = urlSplit[urlSplit.length-2];
			scores[2] = Double.toString(r);
			numList.add(r);
			list.add(count, scores);
			count++;
		}
		//need to sort the list so highest page rank shows on top

		//set list
		GridScrollPane dataPane = new GridScrollPane(colNames, list);
		nodeRankSheet.setContentPane(dataPane);
		
		//set tab on graphplaysheet
		playSheet.jTab.add("NodeRank Scores", nodeRankSheet);
		nodeRankSheet.setClosable(true);
		nodeRankSheet.setMaximizable(true);
		nodeRankSheet.setIconifiable(true);
		nodeRankSheet.setTitle("NodeRank Scores");
		nodeRankSheet.pack();
		nodeRankSheet.setVisible(true);
	}
	
	
			
}
