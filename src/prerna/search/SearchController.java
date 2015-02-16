/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
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
package prerna.search;

import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusEvent;
import java.awt.event.FocusListener;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;

import javax.swing.JPopupMenu;
import javax.swing.JTextField;
import javax.swing.JToggleButton;

import org.apache.jena.larq.IndexLARQ;
import org.apache.jena.larq.IndexWriterFactory;
import org.apache.jena.larq.LARQ;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.RAMDirectory;

import prerna.om.SEMOSSVertex;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.ui.transformer.ArrowFillPaintTransformer;
import prerna.ui.transformer.EdgeStrokeTransformer;
import prerna.ui.transformer.VertexLabelFontTransformer;
import prerna.ui.transformer.VertexPaintTransformer;
import prerna.util.Constants;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;

import edu.uci.ics.jung.visualization.VisualizationViewer;
import edu.uci.ics.jung.visualization.picking.MultiPickedState;
import edu.uci.ics.jung.visualization.picking.PickedState;

/**
 */
public class SearchController implements KeyListener, FocusListener, ActionListener, Runnable {

	static final Logger logger = LogManager.getLogger(SearchController.class.getName());

	private JTextField searchText;
	private Model jenaModel = null;
	SubjectIndexer larqBuilder = null;
	long lastTime = 0;
	Thread thread = null;
	boolean typed = false;
	boolean searchContinue = true;
	JPopupMenu menu = new JPopupMenu();
	Hashtable <String, String> resHash = new Hashtable<String, String>();
	public Hashtable <String, String> cleanResHash = new Hashtable<String, String>();
	VisualizationViewer target = null;
	VertexPaintTransformer oldTx = null;
	EdgeStrokeTransformer oldeTx = null;
	ArrowFillPaintTransformer oldafpTx = null;
	VertexLabelFontTransformer oldVLF = null;
	PickedState liveState;
	PickedState tempState = new MultiPickedState();
	GraphPlaySheet gps;
	JToggleButton btnHighlight;

	/**
	 * Constructor for SearchController.
	 */
	public SearchController(){

	}

	// toggle button listener
	// this will swap the view based on what is being presented
	/**
	 * Method actionPerformed.
	 * @param e ActionEvent
	 */
	public void actionPerformed(ActionEvent e)
	{
		btnHighlight = (JToggleButton) e.getSource();
		// see if the key is depressed
		// if yes swap the transformer
		if(btnHighlight.isSelected())
		{
			//set the transformers
			oldTx = (VertexPaintTransformer)target.getRenderContext().getVertexFillPaintTransformer();
			oldTx.setVertHash(resHash);
			oldeTx = (EdgeStrokeTransformer)target.getRenderContext().getEdgeStrokeTransformer();
			oldeTx.setEdges(null);
			oldafpTx = (ArrowFillPaintTransformer)target.getRenderContext().getArrowFillPaintTransformer();
			oldafpTx.setEdges(null);
			oldVLF = (VertexLabelFontTransformer)target.getRenderContext().getVertexFontTransformer();
			oldVLF.setVertHash(resHash);
			target.repaint();
			//if the search vertex state has been cleared, we need to refill it with what is in the res hash
			Hashtable<String, SEMOSSVertex> vertStore = gps.getGraphData().getVertStore();
			if(tempState.getPicked().size()==0 && !resHash.isEmpty()){
				Iterator resIt = resHash.keySet().iterator();
				while(resIt.hasNext())
					liveState.pick(vertStore.get(resIt.next()), true);
			}
			//if there are vertices in the temp state, need to pick them in the live state and clear tempState
			if(tempState.getPicked().size()>0){
				Iterator resIt = tempState.getPicked().iterator();
				while(resIt.hasNext())
					liveState.pick(resIt.next(), true);
				tempState.clear();
			}
		}
		else
		{
			liveState.clear();
			oldTx.setVertHash(null);
			oldeTx.setEdges(null);
			oldafpTx.setEdges(null);
			oldVLF.setVertHash(null);
			target.repaint();
		}
		gps.resetTransformers();
	}

	/**
	 * Method searchStatement.
	 * @param searchString String
	 */
	public void searchStatement(String searchString)
	{
		logger.debug("Jena Model is " + jenaModel);
		String searchQuery = "PREFIX pf: <http://jena.hpl.hp.com/ARQ/property#>"+
				"SELECT ?doc" +
				"{" +
				" ?doc pf:textMatch " ;
		String remainder =  " 0.25 80). ?doc ?p ?lit }";
		String remainder2 =  " 0.25 80). ?lit ?p ?doc }";
		String remainder3 =  " 0.25 80). ?lit ?doc ?obj }";

		String searchQuery2 = "PREFIX pf: <http://jena.hpl.hp.com/ARQ/property#>" +
				"SELECT distinct ?doc  { ?doc pf:textMatch ("; 

		String cSearchQuery = searchQuery2 + "'" + searchString + "'" + remainder;
		//String cSearchQuery = searchQuery + "'" + data + "'" + remainder;
		logger.debug("Query  " + cSearchQuery);
		com.hp.hpl.jena.query.Query lQuery = QueryFactory.create(cSearchQuery);
		QueryExecution qexec2 = QueryExecutionFactory.create(lQuery, jenaModel);

		ResultSet rs = qexec2.execSelect();
		resHash.clear();
		cleanResHash.clear();
		tempState.clear();
		menu.setSize(new Dimension(410, 60));
		menu.add("Results to be highlighted......");
		Hashtable<String, SEMOSSVertex> vertStore = gps.getGraphData().getVertStore();
		synchronized(menu)
		{
			while(rs.hasNext())
			{
				QuerySolution qs = rs.next();
				String doc = qs.get("doc") + "";
				logger.debug("Document is " + doc);
				resHash.put(doc, doc);
				cleanResHash.put(doc, doc);

				tempState.pick(vertStore.get(doc), true);
				menu.add(doc);
			}
		}
		qexec2.close();
		// execute query for objects

		cSearchQuery = searchQuery2 + "'" + searchString + "'" + remainder2;
		//String cSearchQuery = searchQuery + "'" + data + "'" + remainder;
		logger.debug("Query  " + cSearchQuery);
		lQuery = QueryFactory.create(cSearchQuery);
		qexec2 = QueryExecutionFactory.create(lQuery, jenaModel);

		rs = qexec2.execSelect();
		synchronized(menu)
		{
			while(rs.hasNext())
			{
				QuerySolution qs = rs.next();
				String doc = qs.get("doc") + "";
				logger.debug("Document is " + doc);
				resHash.put(doc, doc);
				cleanResHash.put(doc, doc);

				tempState.pick(vertStore.get(doc), true);
				menu.add(doc);
			}
		}
		qexec2.close();

		target.repaint();
		searchText.requestFocus(true);
	}

	/**
	 * Method run.
	 */
	public void run()
	{
		try {
			while(searchContinue)
			{
				long thisTime = System.currentTimeMillis();
				if(thisTime - lastTime > 300 && typed)
				{
					synchronized(menu)
					{
						menu.setVisible(false);
						menu.removeAll();
					}
					if(searchText.getText().length() > 0 && lastTime != 0)
						searchStatement(searchText.getText());
					else if(searchText.getText().length() == 0 && lastTime != 0){
						resHash.clear();
						cleanResHash.clear();
						tempState.clear();
						logger.debug("cleared");
					}
					lastTime = System.currentTimeMillis();
					typed = false;
				}
				else
				{
					//menu.setVisible(false);
					Thread.sleep(100);
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	// key listener
	/**
	 * Method keyTyped.
	 * @param e KeyEvent
	 */
	@Override
	public void keyTyped(KeyEvent e) {

		lastTime = System.currentTimeMillis();
		menu.setVisible(false);
		typed = true;
		//e.getKeyChar() >= 'a' && e.getKeyChar() <= 'b' && 
		// e.getKeyChar() != KeyEvent.VK_BACK_SPACE && e.getKeyChar() != KeyEvent.VK_DELETE)
		/*if(e.getID() == KeyEvent.KEY_TYPED && e.getKeyChar() != KeyEvent.VK_BACK_SPACE && e.getKeyChar() != KeyEvent.VK_DELETE)
		{
			data = data + e.getKeyChar();
			//searchText.setText(data);
		// create a thread here which will update the vector by searching
		}
		else if(e.getKeyChar() == KeyEvent.VK_BACK_SPACE)
		{
			//remove the last character
			//logger.debug("Invoked " + data.length());
			if(data.length() > 0)
			{
				data = data.substring(0,data.length() - 1);
				//searchText.setText(data);
			}
		}
		/*else if(e.getKeyChar() == KeyEvent.VK_DELETE)
		{
			// delete from this point on
			// see if there is anything is in the clipboard
			String selText = searchText.getSelectedText();
			if(selText.length() >= 0)
			{
				data = data.replace(selText, "");
				searchText.setText(data);
			}
			// else delete from this point on
			else
			{
				int curPosition = searchText.getCaretPosition();
				// find the substring until this position
				if(curPosition < searchText.getText().length())
				{
					String data1 = data.substring(0, curPosition);
					String data2 = data.substring(curPosition + 1);
					data = data1 + data2;
					searchText.setText(data);
				}
			}
		}*/
		/*data = searchText.getText(); */
//		synchronized(this)
//		{
//			this.notify();
//		}
		/*if(data.length() > 0)
			searchStatement(data);*/
	}


	// focus listener
	/**
	 * Method focusGained.
	 * @param e FocusEvent
	 */
	@Override
	public void focusGained(FocusEvent e) {
		if(searchText.getText().equalsIgnoreCase(Constants.ENTER_TEXT))
			searchText.setText("");
		if(thread == null || thread.getState() == Thread.State.TERMINATED)
		{
			thread = new Thread(this);
			searchContinue = true;
			thread.start();
			logger.info("Starting thread again");
		}
	}

	/**
	 * Method focusLost.
	 * @param e FocusEvent
	 */
	@Override
	public void focusLost(FocusEvent e) {
		if(searchText.getText().equalsIgnoreCase(""))
		{
			searchText.setText(Constants.ENTER_TEXT);
			searchContinue = false;
			logger.info("Ended the thread");
		}
	}

	/**
	 * Method indexStatements.
	 * @param jenaModel Model
	 */
	public void indexStatements(Model jenaModel)
	{
		try
		{
			IndexWriter iw = IndexWriterFactory.create(new RAMDirectory());
			//larqBuilder = new IndexBuilderSubject(iw);
			larqBuilder = new SubjectIndexer(iw);
			larqBuilder.indexStatements(jenaModel.listStatements());
			larqBuilder.closeWriter();
			IndexLARQ larq = larqBuilder.getIndex();
			LARQ.setDefaultIndex(larq);
			this.jenaModel = jenaModel;
		}catch(RuntimeException ex)
		{
			ex.printStackTrace();
		} catch (CorruptIndexException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (LockObtainFailedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	/**
	 * Method setText.
	 * @param text JTextField
	 */
	public void setText(JTextField text){
		this.searchText = text;
	}

	/**
	 * Method setGPS.
	 * @param ps GraphPlaySheet
	 */
	public void setGPS(GraphPlaySheet ps){
		this.gps = ps;
	}

	/**
	 * Method setState.
	 * @param ps PickedState
	 */
	public void setState(PickedState ps){
		this.liveState = ps;
	}

	/**
	 * Method setTarget.
	 * @param vv VisualizationViewer
	 */
	public void setTarget(VisualizationViewer vv){
		this.target = vv;
	}

	/**
	 * Method keyPressed.
	 * @param arg0 KeyEvent
	 */
	@Override
	public void keyPressed(KeyEvent arg0) {

	}

	/**
	 * Method keyReleased.
	 * @param arg0 KeyEvent
	 */
	@Override
	public void keyReleased(KeyEvent arg0) {

	}
}

