/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;

import javax.swing.JInternalFrame;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.impl.PageRankCalculator;
import prerna.om.SEMOSSVertex;
import prerna.ui.components.GridFilterData;
import prerna.ui.components.GridScrollPane;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.QuestionPlaySheetStore;

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
		
		PageRankCalculator calc = new PageRankCalculator();
		Hashtable<SEMOSSVertex, Double> ranks = calc.calculatePageRank(playSheet.forest);

		ArrayList <Object []> list = new ArrayList();
		int count = 0;
		
		Collection<String> col = playSheet.forest.getVertices();
		Iterator it = col.iterator();

		//process through graph and list out all nodes, type, pagerank
		while (it.hasNext()) {
			SEMOSSVertex v= (SEMOSSVertex) it.next();
			String url = v.getURI();
			String[] urlSplit = url.split("/");
			double r = ranks.get(v);
			
			String [] scores = new String[3];
			scores[0] = urlSplit[urlSplit.length-1];
			scores[1] = urlSplit[urlSplit.length-2];
			scores[2] = Double.toString(r);
			list.add(count, scores);
			count++;
		}
		
		GridFilterData gfd = new GridFilterData();
		JInternalFrame nodeRankSheet = new JInternalFrame();
		String[] colNames = new String[3];
		colNames[0] = "Vertex Name";
		colNames[1] = "Vertex Type";
		colNames[2] = "Page Rank Score";
		gfd.setColumnNames(colNames);
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
