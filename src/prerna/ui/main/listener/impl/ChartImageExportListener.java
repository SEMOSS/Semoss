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
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

import com.itextpdf.text.Document;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Rectangle;
import com.itextpdf.text.pdf.PdfWriter;
import com.teamdev.jxbrowser.chromium.Browser;
import com.teamdev.jxbrowser.chromium.swing.BrowserView;

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

	final double MAX_DIM = 14400;
	final String HIGH_QUALITY_IMAGE_TYPE = "EPS";
	final String LOW_QUALITY_IMAGE_TYPE = "PNG";
	final String PDF_TYPE = "PDF";
	
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
		BrowserView browserView = bps.getBrowserView();
		Image i = browserView.getImage();
		
		FileOutputStream graphicsFileOut = null;
		try {
			String exportType = "";
			if(!autoExport)
			{
				//Display dialog to choose export quality
				Object[] options = {"High Quality (*.eps)", "Low Quality (*.png)", "PDF"};
				int n = JOptionPane.showOptionDialog(splitPane,"Please choose the type of export: ",
						"Export", JOptionPane.YES_NO_CANCEL_OPTION, JOptionPane.QUESTION_MESSAGE, null, options, options[1]);
				
				switch(n) {
					case -1: return;
					case 0: exportType = this.HIGH_QUALITY_IMAGE_TYPE;
										 break;
					case 1: exportType = this.LOW_QUALITY_IMAGE_TYPE;
										 break;
					case 2: exportType = this.PDF_TYPE;
										 break;
				}
			}
			
			if(fileLoc.isEmpty())
			{
				String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
				String folder = System.getProperty("file.separator")+"export"+System.getProperty("file.separator")+"Images"+System.getProperty("file.separator");
				String writeFileName = "Graph_Export_" + DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT).format(new Date()).replace(":", "");
				if(exportType.equals(this.HIGH_QUALITY_IMAGE_TYPE)) {
					writeFileName += ".eps";
				} else {
					writeFileName += ".png";
				}
				
				//Create folders if necessary to export to correct path
				fileLoc = workingDir + folder;
				File exportFile = new File(fileLoc);
				if(!exportFile.getCanonicalFile().isDirectory()) {
					if(!exportFile.mkdirs())
					Utility.showError("Please create the following directory structure in the working folder where SEMOSS currently resides:\n\n~"+System.getProperty("file.separator")+"export"+System.getProperty("file.separator")+"Images"+System.getProperty("file.separator"));
					return;
				}
				fileLoc += writeFileName;
			}
			
			int imageWidth = i.getWidth(null);
	        int imageHeight = i.getHeight(null);
			
	        BufferedImage dest;
	        //Export chart based upon user-chosen quality value
			if(exportType.equals(this.HIGH_QUALITY_IMAGE_TYPE)) {
				graphicsFileOut = new FileOutputStream(fileLoc);
				Graphics2D g = new EpsGraphics2D("Chart Export", graphicsFileOut, 0, 0, imageWidth, imageHeight);
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
	        	
	        	// PDF Export
	        	if(exportType.equals(this.PDF_TYPE)) {
	        		FileOutputStream fileOut = null;
					try {
						com.itextpdf.text.Image image1 = com.itextpdf.text.Image.getInstance(fileLoc);
						Rectangle r;
						if(image1.getHeight() > this.MAX_DIM) {
							r = new Rectangle((int)image1.getWidth(), (int) MAX_DIM);
						} else if(image1.getWidth() > this.MAX_DIM) {
							r = new Rectangle((int) MAX_DIM, (int)image1.getHeight());
						} else {
							r = new Rectangle((int)image1.getWidth()+20, (int)image1.getHeight()+20);
						}

						Document document = new Document(r, 15, 25, 15, 25);
						fileOut = new FileOutputStream(fileLoc.replace(this.LOW_QUALITY_IMAGE_TYPE.toLowerCase(), this.PDF_TYPE.toLowerCase()));
						PdfWriter.getInstance(document, fileOut);
						document.open();

						int pages = (int) Math.ceil((double)dest.getHeight() / this.MAX_DIM);
						if(pages == 0)
							pages = 1;
						for(int j = 0; j < pages; j++) {
							BufferedImage temp;
							if(j < pages-1) {
								temp = dest.getSubimage(0, j*(int)this.MAX_DIM, dest.getWidth(), (int)this.MAX_DIM);
							} else {
								temp = dest.getSubimage(0, j*(int)this.MAX_DIM, dest.getWidth(), dest.getHeight() % (int)this.MAX_DIM);
							}
							File tempFile = new File(i+this.LOW_QUALITY_IMAGE_TYPE);
							ImageIO.write(temp, this.LOW_QUALITY_IMAGE_TYPE, tempFile);
							com.itextpdf.text.Image croppedImage = com.itextpdf.text.Image.getInstance(i+this.LOW_QUALITY_IMAGE_TYPE);
							document.add(croppedImage);
							tempFile.delete();
							
							if(j < pages-1) {
								document.newPage();
							}
						}
						document.close();

						File f = new File(fileLoc);
						f.delete();
					} catch(RuntimeException | DocumentException e) {
						e.printStackTrace();
					}finally{
						try{
							if(fileOut!=null)
								fileOut.close();
						}catch(IOException e) {
							e.printStackTrace();
						}
					}
				} // end if PDF
			}
	        if(!autoExport) {
	        	if(exportType.equals(this.PDF_TYPE)) {
					Utility.showMessage("Export successful: " + fileLoc.replace(this.LOW_QUALITY_IMAGE_TYPE.toLowerCase(), this.PDF_TYPE.toLowerCase()));
				} else {
					Utility.showMessage("Export successful: " + fileLoc);
				}
	        }
		} catch (IOException e) {
			Utility.showError("Graph export failed.");
			e.printStackTrace();
		}finally{
			try{
				if(graphicsFileOut!=null)
					graphicsFileOut.close();
			}catch(IOException e) {
				e.printStackTrace();
			}
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
