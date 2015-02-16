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
package prerna.ui.components.playsheets;

import java.util.Hashtable;

import javax.swing.JComponent;
import javax.swing.JInternalFrame;
import javax.swing.JMenuItem;
import javax.swing.JPopupMenu;
import javax.swing.JProgressBar;
import javax.swing.Painter;
import javax.swing.UIDefaults;
import javax.swing.UIManager;
import javax.swing.plaf.basic.BasicInternalFrameUI;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.main.listener.impl.EditPlaySheetTitleListener;

/**
 * The AbstractRDFPlaySheet class creates the structure of the basic components of the Play Sheet, including the RDFEngine, the query, and the panel.
 */
public abstract class AbstractRDFPlaySheet extends JInternalFrame implements IPlaySheet {

	protected boolean overlay = false;
	protected String query = null;
	protected String title = null;
	public IEngine engine = null;
	protected String questionNum = null;
	public JComponent pane = null;
	private static final Logger logger = LogManager.getLogger(AbstractRDFPlaySheet.class.getName());
	public JProgressBar jBar = new JProgressBar();
	String playsheetType = null;
	
	/**
	 * Constructor for AbstractRDFPlaySheet.
	 */
	public AbstractRDFPlaySheet(){
		//super("_", true, true, true, true);
	}
	
	/**
	 * Method run.  Calls createView() and creates the first instance of a play sheet.
	 */
	@Override
	public void run() {
		createView();		
	}
	
	public abstract Hashtable<String, String> getDataTableAlign();
	
	public Object getData(){
		Hashtable retHash = new Hashtable();
		retHash.put("id", this.questionNum==null? "": this.questionNum);
		String className = "";
		Class<?> enclosingClass = getClass().getEnclosingClass();
		if (enclosingClass != null) {
			className = enclosingClass.getName();
		} else {
			className = getClass().getName();
		}
		retHash.put("playsheet", className);
		retHash.put("title", this.title==null? "": this.title);
		Hashtable<String, String> tableAlign = getDataTableAlign();
		if(tableAlign != null)
			retHash.put("dataTableAlign", tableAlign);
		
		return retHash;
	}
	
	public void setAppend(boolean overlay) {
		this.overlay = overlay;	
	}
	/**
	 * Gets the latest query set to the play sheet.  <p> If multiple queries have been set to the specific play sheet through
	 * Extend or Overlay, the function will return the last query set to the play sheet.
	
	 * @see #extendView()
	 * @see #overlayView()
	 * @return the SPARQL query previously set to this play sheet */
	@Override
	public String getQuery() {
		return query;
	}

	/**
	 * Sets the String version of the SPARQL query on the play sheet. <p> The query must be set before creating the model for
	 * visualization.  Thus, this function is called before createView(), extendView(), overlayView()--everything that 
	 * requires the play sheet to pull data through a SPARQL query.
	 * @param query the full SPARQL query to be set on the play sheet
	 * @see	#createView()
	 * @see #extendView()
	 * @see #overlayView()
	 */
	@Override
	public void setQuery(String query) {
		logger.info("New Query " + query);
		query = query.trim();
		this.query = query;
	}

	/**
	 * Sets the title of the play sheet.  The title is displayed as the text on top of the internal frame that is the play sheet.
	 * @param title representative name for the play sheet.  Often a concatenation of the question ID and question text
	 */
	@Override
	public void setTitle(String title)
	{
		super.setTitle(title);
		this.title = title;
	}

	/**
	 * Gets the title of the play sheet.  The title is displayed as the text on top of the internal frame that is the play sheet.
	 * @return String representative name for the play sheet.  Often a concatenation of the question ID and question text
	 */
	@Override
	public String getTitle()
	{
		return title;
	}

	/**
	 * Sets the RDF engine for the play sheet to run its query against.  Can be any of the active engines, all of which are 
	 * stored in DIHelper
	 * @param engine the active engine for the play sheet to run its query against.
	 */
	@Override
	public void setRDFEngine(IEngine engine)
	{
		logger.info("Set the engine " );
		this.engine = engine;
	}

	/**
	 * Gets the RDF engine for the play sheet to run its query against.  Can be any of the active engines, all of which are 
	 * stored in DIHelper
	
	
	 * @return IEngine */
	@Override
	public IEngine getRDFEngine()
	{
		return this.engine;
	}
	
	/**
	 * Sets the identification code for the question associated with the play sheet.  The question ID is what can be used in 
	 * conjunction with the specified engine's properties file to get the SPARQL query associated with the question as well 
	 * as the question String and play sheet class that the question is to be run on.
	 * @param id representation of the question as laid out in the specified engine's question file
	 */
	@Override
	public void setQuestionID(String questionNum)
	{
		this.questionNum = questionNum;
	}

	/**
	 * Returns the question identification code that has been set to the play sheet. The question identification code can be used
	 * in conjunction with the specified engine's properties file to get the SPARQL query associated with the question as well
	 * as the question String and the play sheet class that the question is to be run on.  Only one question ID can be set to 
	 * each play sheet, so after using {@link #extendView()} or {@link #overlayView()} the play sheet will have the most 
	 * recent question ID associated with it.
	
	 * @return questionID the identification code of the question associated with the play sheet */
	@Override
	public String getQuestionID()
	{
		return this.questionNum;
	}


	/**
	 * Sets the JDesktopPane to display the play sheet on. <p> This must be set before calling functions like 
	 * {@link #createView()} or {@link #extendView()}, as functions like these add the panel to the desktop pane set in this
	 * function.
	 * @param pane the desktop pane that the play sheet is to be displayed on
	 */
	@Override
	public void setJDesktopPane(JComponent pane)
	{
		this.pane = pane;
	}
	
	/**
	 * Updates the progress bar to display a specified status and progress value.
	 * 
	 * @param status The text to be displayed on the progress bar. 
	 * @param x The value of the progress bar.
	 */
	public void updateProgressBar(String status, int x)
	{
		jBar.setString(status);
		jBar.setValue(x);

	}	
	
	public void createView()
	{
		// will be taken care of in the individual play sheets
	}
	
	protected void setWindow(){
		setResizable(true);
		setClosable(true);
		setMaximizable(true);
		setIconifiable(true);
		JPopupMenu popup = this.getComponentPopupMenu();
		JMenuItem editTitle = new JMenuItem("Edit Title");
		EditPlaySheetTitleListener mylistener = new EditPlaySheetTitleListener();
		mylistener.setPlaySheet(this);
		editTitle.addActionListener(mylistener);
		popup.add(editTitle, 0);
		if(((BasicInternalFrameUI) this.getUI()).getNorthPane().getComponentCount() > 3) // only want to remove the dropdown button--not the close and minimize etc.
			((BasicInternalFrameUI) this.getUI()).getNorthPane().remove(0);
	}

	/**
	 * Resets the progress bar to the default setting.
	 */
	public void resetProgressBar()
	{
		UIDefaults nimbusOverrides = new UIDefaults();
		UIDefaults defaults = UIManager.getLookAndFeelDefaults();
		defaults.put("nimbusOrange",defaults.get("nimbusInfoBlue"));
		Painter blue = (Painter) defaults.get("Button[Default+Focused+Pressed].backgroundPainter");
        nimbusOverrides.put("ProgressBar[Enabled].foregroundPainter",blue);
        jBar.putClientProperty("Nimbus.Overrides", nimbusOverrides);
        jBar.putClientProperty("Nimbus.Overrides.InheritDefaults", false);
		jBar.setStringPainted(true);
	}
}
