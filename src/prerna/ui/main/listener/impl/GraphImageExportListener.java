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

import java.awt.Component;
import java.awt.Graphics2D;
import java.awt.event.ActionEvent;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.util.Date;

import javax.imageio.ImageIO;
import javax.swing.AbstractAction;
import javax.swing.JComponent;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JTabbedPane;

import org.sourceforge.jlibeps.epsgraphics.EpsGraphics2D;

import prerna.ui.components.LegendPanel2;
import prerna.ui.components.api.IChakraListener;
import prerna.util.Utility;

/**
 * Controls the export of a graph to PNG/EPS image formats.
 */
public class GraphImageExportListener extends AbstractAction implements IChakraListener {

	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param arg0 ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		Component source = (Component) arg0.getSource(); //Export (>>) button
		JPanel cont = (JPanel) source.getParent().getParent().getParent().getParent(); //Overall container JPanel
		
		JPanel jp = (JPanel) cont.getComponent(0);
		JTabbedPane tabbedPane = (JTabbedPane) cont.getComponent(1);
		
		LegendPanel2 legend = (LegendPanel2) jp.getComponent(0);
		JProgressBar progressBar = (JProgressBar) jp.getComponent(1);
		
		Object[] options = {"High Quality (*.eps)", "Low Quality (*.png)"};
		int n = JOptionPane.showOptionDialog(cont,"Please choose the type of export: ",
				"Export", JOptionPane.YES_NO_CANCEL_OPTION, JOptionPane.QUESTION_MESSAGE, null, options, options[1]);
		if(n == -1) {
			return;
		}
		boolean highQuality = n == 0 ? true : false;
		
		try {
			String workingDir = System.getProperty("user.dir");
			String folder = "\\export\\Images\\";
			String writeFileName = "Graph_Export_" + DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT).format(new Date()).replace(":", "");
			if(highQuality) {
				writeFileName += ".eps";
			} else {
				writeFileName += ".png";
			}
			
			//Create folders if necessary to export to correct path
			String fileLoc = workingDir + folder;
			File exportFile = new File(fileLoc);
			if(!exportFile.getCanonicalFile().isDirectory()) {
				if(!exportFile.mkdirs())
				Utility.showError("Please create the following directory structure in the working folder where SEMOSS currently resides:\n\n~\\export\\Images\\");
				return;
			}
			fileLoc += writeFileName;
			
			int boundingBoxMinWidth = 0;
			int boundingBoxMinHeight = 0; //Crop out search panel
			int boundingBoxMaxWidth = cont.getWidth();
			int boundingBoxMaxHeight = cont.getHeight() - progressBar.getHeight(); //Crop out progress bar
			
			if(highQuality) {
				Graphics2D g = new EpsGraphics2D("Graph Export", new FileOutputStream(fileLoc), boundingBoxMinWidth, boundingBoxMinHeight, boundingBoxMaxWidth, boundingBoxMaxHeight);
				cont.paint(g);
			} else {
				BufferedImage im = new BufferedImage(boundingBoxMaxWidth, boundingBoxMaxHeight, BufferedImage.TYPE_INT_ARGB);
				cont.paint(im.getGraphics());
				ImageIO.write(im, "PNG", new File(fileLoc));
			}
			
			Utility.showMessage("Graph export successful: " + fileLoc);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {
	}
}
