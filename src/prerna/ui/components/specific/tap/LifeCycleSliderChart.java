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
package prerna.ui.components.specific.tap;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.beans.PropertyVetoException;
import java.util.Hashtable;
import java.util.concurrent.TimeUnit;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JTable;
import javax.swing.Painter;
import javax.swing.UIDefaults;
import javax.swing.UIManager;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.ui.main.listener.impl.BrowserZoomKeyListener;
import prerna.ui.main.listener.impl.PlaySheetListener;
import prerna.util.Constants;
import prerna.util.DIHelper;

import com.google.gson.Gson;
import com.teamdev.jxbrowser.chromium.Browser;
import com.teamdev.jxbrowser.chromium.BrowserFactory;

/**
 * This class creates the chart for the lifecycle slider.
 */
@SuppressWarnings("serial")
public class LifeCycleSliderChart extends GridPlaySheet{

	//public JPanel cheaterPanel = new JPanel();
	public Browser browser = BrowserFactory.create();
	Hashtable <String, String[]> hardwareHash;
	String fileName;

	
	/**
	 * Constructor for LifeCycleSliderChart.
	 */
	public LifeCycleSliderChart() {
		super();

		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/lifecycle.html";
	}
	
	/**
	 * Creates a panel and adds the table to it.
	 * Checks for the type of browser and creates the appropriate display.
	 */
	@Override
	public void addPanel()
	{
		setWindow();
		try {
			table = new JTable();
			JPanel mainPanel = new JPanel();
			PlaySheetListener psListener = new PlaySheetListener();
			this.addInternalFrameListener(psListener);
			this.setContentPane(mainPanel);
			GridBagLayout gbl_mainPanel = new GridBagLayout();
			gbl_mainPanel.columnWidths = new int[]{0, 0};
			gbl_mainPanel.rowHeights = new int[]{0, 0};
			gbl_mainPanel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
			gbl_mainPanel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
			mainPanel.setLayout(gbl_mainPanel);

			//callIt(table);
			JPanel panel = new JPanel();
			panel.setLayout(new BorderLayout());
			panel.add(browser.getView().getComponent(), BorderLayout.CENTER);
			GridBagConstraints gbc_scrollPane = new GridBagConstraints();
			gbc_scrollPane.fill = GridBagConstraints.BOTH;
			gbc_scrollPane.gridx = 0;
			gbc_scrollPane.gridy = 0;
			mainPanel.add(panel, gbc_scrollPane);

			jBar.setStringPainted(true);
			jBar.setString("0%...Preprocessing");
			jBar.setValue(0);
			UIDefaults nimbusOverrides = new UIDefaults();
			UIDefaults defaults = UIManager.getLookAndFeelDefaults();
			defaults.put("nimbusOrange",defaults.get("nimbusInfoBlue"));
			Painter blue = (Painter) defaults.get("Button[Default+Focused+Pressed].backgroundPainter");
			nimbusOverrides.put("ProgressBar[Enabled].foregroundPainter",blue);
			jBar.putClientProperty("Nimbus.Overrides", nimbusOverrides);
			jBar.putClientProperty("Nimbus.Overrides.InheritDefaults", false);
			JPanel barPanel = new JPanel();

			GridBagConstraints gbc_barPanel = new GridBagConstraints();
			gbc_barPanel.fill = GridBagConstraints.BOTH;
			gbc_barPanel.gridx = 0;
			gbc_barPanel.gridy = 1;
			mainPanel.add(barPanel, gbc_barPanel);
			barPanel.setLayout(new BorderLayout(0, 0));
			barPanel.add(jBar, BorderLayout.CENTER);

			pane.add(this);

			this.pack();
			this.setVisible(true);
			this.setSelected(false);
			this.setSelected(true);
			logger.debug("Added the main pane");
		}
		catch (PropertyVetoException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Creates the view. Updates the progress bar while RDF statements are processed from the repository.
	 */
	@Override
	public void createView() {
		addPanel();
		SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		if(engine!= null && rs == null){
			wrapper.setQuery(query);
			updateProgressBar("10%...Querying RDF Repository", 10);
			wrapper.setEngine(engine);
			updateProgressBar("30%...Querying RDF Repository", 30);
			wrapper.executeQuery();
			updateProgressBar("60%...Processing RDF Statements	", 60);
		}
		else if (engine==null && rs!=null){
			wrapper.setResultSet(rs);
			wrapper.setEngineType(IEngine.ENGINE_TYPE.JENA);
		}
		
		// get the bindings from it
		String [] colNames = wrapper.getVariables();
		hardwareHash = new Hashtable<String, String[]>();		

		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				SesameJenaSelectStatement sjss = wrapper.next();
				
				String[] values = new String[colNames.length-1];
				String key = sjss.getVar(colNames[0]).toString();
				for(int colIndex = 1;colIndex < colNames.length;colIndex++)
				{
					String val = sjss.getVar(colNames[colIndex]).toString();
					val=val.replaceAll("\"", "");
					values[colIndex-1] = val;
				}
				hardwareHash.put(key,values);
			}
			updateProgressBar("80%...Creating Visualization", 80);
			
		} catch (Exception e) {
			logger.fatal(e);
		}
		updateProgressBar("100%...Table Generation Complete", 100);
		showAll();
		browser.getView().getComponent().addKeyListener(new BrowserZoomKeyListener(browser));
		browser.loadURL(fileName);
		while (browser.isLoading()) {
		    try {
				TimeUnit.MILLISECONDS.sleep(50);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		callIt();
	}
	
	/**
	 * This method is used to convert a hashtable via GSON. 
	 * GSON is a Java library used to convert Java objects into their JSON representations. 
	 * This value is then passed to Javascript.

	 */
	public void callIt()
	{
		Hashtable<String, String[]> newHash = hardwareHash;
		Gson gson = new Gson();
		logger.info("Converted " + gson.toJson(newHash));
	    browser.executeJavaScript("start('" + gson.toJson(newHash) + "');");
	}

	/**
	 * Shows all properties on the desktop pane.
	 */
	public void showAll() {
//		JDesktopPane pane = (JDesktopPane) DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);

		this.setVisible(true);

		this.pack();

		JFrame frame2 = (JFrame) DIHelper.getInstance().getLocalProp(
				Constants.MAIN_FRAME);
		frame2.repaint();
		
		
	}


	
}

