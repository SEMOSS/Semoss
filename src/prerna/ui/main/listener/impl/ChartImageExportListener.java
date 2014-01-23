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
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Image;
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
import javax.swing.JSplitPane;

import org.sourceforge.jlibeps.epsgraphics.EpsGraphics2D;

import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.playsheets.BrowserPlaySheet;
import prerna.util.Utility;

import com.teamdev.jxbrowser.Browser;

/**
 * Controls the export of a chart to a vector image.
 */
public class ChartImageExportListener extends AbstractAction implements IChakraListener {
	
	Boolean autoExport = false;
	Boolean crop = false;
	Boolean scale = false;
	String fileLoc ="";
	int cropX = -1;
	int cropY = -1;
	int cropWidth = -1;
	int cropHeight = -1;
	int scaleWidth = -1;
	int scaleHeight = -1;
	
	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param arg0 ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		Component source = (Component) arg0.getSource(); //Export button
		JSplitPane splitPane = (JSplitPane) source.getParent().getParent();
		BrowserPlaySheet bps = (BrowserPlaySheet) splitPane.getParent().getParent().getParent(); //Parents: JLayeredPane - JRootPane - Subclass of BrowserPlaySheet
		Browser browser = bps.getBrowser();
		Image i = browser.toImage(true);
		
		try {
			boolean highQuality = false;
			if(!autoExport)
			{
				//Display dialog to choose export quality
				Object[] options = {"High Quality (*.eps)", "Low Quality (*.png)"};
				int n = JOptionPane.showOptionDialog(splitPane,"Please choose the type of export: ",
						"Export", JOptionPane.YES_NO_CANCEL_OPTION, JOptionPane.QUESTION_MESSAGE, null, options, options[1]);
				if(n == -1) {
					return;
				}
				highQuality = n == 0 ? true : false;
			}	
			
			if(fileLoc.isEmpty())
			{
				String workingDir = System.getProperty("user.dir");
				String folder = "\\export\\Images\\";
				String writeFileName = "Graph_Export_" + DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT).format(new Date()).replace(":", "");
				if(highQuality) {
					writeFileName += ".eps";
				} else {
					writeFileName += ".png";
				}
				
				//Create folders if necessary to export to correct path
				fileLoc = workingDir + folder;
				File exportFile = new File(fileLoc);
				if(!exportFile.getCanonicalFile().isDirectory()) {
					if(!exportFile.mkdirs())
					Utility.showError("Please create the following directory structure in the working folder where SEMOSS currently resides:\n\n~\\export\\Images\\");
					return;
				}
				fileLoc += writeFileName;
			}
			
			
			int imageWidth = i.getWidth(null);
	        int imageHeight = i.getHeight(null);
			
	        BufferedImage dest;
	        //Export chart based upon user-chosen quality value
			if(highQuality) {
				Graphics2D g = new EpsGraphics2D("Chart Export", new FileOutputStream(fileLoc), 0, 0, imageWidth, imageHeight);
				g.drawImage(i, 0, 0, null);
			} else {
				if(!scale)
				{
					dest = new BufferedImage(imageWidth, imageHeight, BufferedImage.TYPE_INT_ARGB);				
					Graphics g2 = dest.createGraphics();
					g2.drawImage(i, 0, 0, null);
				}
				else
				{
					dest=new BufferedImage(scaleWidth, scaleHeight, BufferedImage.TYPE_INT_ARGB);
					Graphics g2 = dest.createGraphics();
					g2.drawImage(i.getScaledInstance(scaleWidth, scaleHeight, Image.SCALE_SMOOTH), 0, 0, null);
				}
		        if(crop)
		        	dest = dest.getSubimage(cropX,cropY,cropWidth,cropHeight);
	        	ImageIO.write(dest, "PNG", new File(fileLoc));
			}
	        if(!autoExport)
	        	Utility.showMessage("Graph export successful: " + fileLoc);
		} catch (IOException e) {
			Utility.showError("Graph export failed.");
			e.printStackTrace();
		}
	}
	
	/**
	 * Method setFileLoc.  Set the location for the image file.
	 * @param newFileLoc String
	 */
	public void setFileLoc(String newFileLoc)
	{
		fileLoc=newFileLoc;
	}

	/**
	 * Method setAutoExport.  Set the autoexport feature.
	 * @param newAuto Boolean
	 */
	public void setAutoExport(Boolean newAuto)
	{
		autoExport = newAuto;
	}
	/**
	 * Method setCropBool.  Sets whether or not to crop the image.
	 * @param newCrop Boolean
	 */
	public void setCropBool(Boolean newCrop)
	{
		crop = newCrop;
	}
	/**
	 * Method setScaleBool.  Sets if the image is scalable or not.
	 * @param newScale Boolean
	 */
	public void setScaleBool(Boolean newScale)
	{
		scale = newScale;
	}
	/**
	 * Method setCrop.  Sets the crop dimensions.
	 * @param x int
	 * @param y int
	 * @param w int
	 * @param h int
	 */
	public void setCrop(int x, int y, int w, int h)
	{
		cropX=x;
		cropY=y;
		cropWidth=w;
		cropHeight=h;
	}
	/**
	 * Method setScale.  Sets the scale of the image.
	 * @param width int
	 * @param height int
	 */
	public void setScale(int width,int height)
	{
		scaleWidth=width;
		scaleHeight=height;
	}
	
	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {
		
	}
}
