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

import com.itextpdf.text.Document;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Image;
import com.itextpdf.text.Rectangle;
import com.itextpdf.text.pdf.PdfWriter;

import prerna.ui.components.LegendPanel2;
import prerna.ui.components.api.IChakraListener;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * Controls the export of a graph to PNG/EPS image formats.
 */
public class GraphImageExportListener extends AbstractAction implements IChakraListener {
	
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
		Component source = (Component) arg0.getSource(); //Export (>>) button
		JPanel cont = (JPanel) source.getParent().getParent().getParent().getParent(); //Overall container JPanel
		
		JPanel jp = (JPanel) cont.getComponent(0);
		JTabbedPane tabbedPane = (JTabbedPane) cont.getComponent(1);
		
		LegendPanel2 legend = (LegendPanel2) jp.getComponent(0);
		JProgressBar progressBar = (JProgressBar) jp.getComponent(1);
		
		Object[] options = {"High Quality (*.eps)", "Low Quality (*.png)", "PDF"};
		//0 = High Quality, 1 = Low Quality, 2 = PDF
		int n = JOptionPane.showOptionDialog(cont,"Please choose the type of export: ",
				"Export", JOptionPane.YES_NO_CANCEL_OPTION, JOptionPane.QUESTION_MESSAGE, null, options, options[1]);
		
		String exportType = "";
		switch(n) {
			case -1: return;
			case 0: exportType = this.HIGH_QUALITY_IMAGE_TYPE;
								 break;
			case 1: exportType = this.LOW_QUALITY_IMAGE_TYPE;
								 break;
			case 2: exportType = this.PDF_TYPE;
								 break;
		}
		
		FileOutputStream graphicsFileOut = null;
		try {
			String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			String folder = System.getProperty("file.separator")+"export"+System.getProperty("file.separator")+"Images"+System.getProperty("file.separator");
			String writeFileName = "Graph_Export_" + DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT).format(new Date()).replace(":", "");
			if(exportType.equals(this.HIGH_QUALITY_IMAGE_TYPE)) {
				writeFileName += ".eps";
			} else {
				writeFileName += ".png";
			}
			
			//Create folders if necessary to export to correct path
			String fileLoc = workingDir + folder;
			File exportFile = new File(fileLoc);
			if(!exportFile.getCanonicalFile().isDirectory()) {
				if(!exportFile.mkdirs())
				Utility.showError("Please create the following directory structure in the working folder where SEMOSS currently resides:\n\n~"+System.getProperty("file.separator")+"export"+System.getProperty("file.separator")+"Images"+System.getProperty("file.separator"));
				return;
			}
			fileLoc += writeFileName;
			
			int boundingBoxMinWidth = 0;
			int boundingBoxMinHeight = 0; //Crop out search panel
			int boundingBoxMaxWidth = cont.getWidth();
			int boundingBoxMaxHeight = cont.getHeight() - progressBar.getHeight(); //Crop out progress bar
			
			if(exportType.equals(this.HIGH_QUALITY_IMAGE_TYPE)) {
				graphicsFileOut = new FileOutputStream(fileLoc);
				Graphics2D g = new EpsGraphics2D("Graph Export",graphicsFileOut, boundingBoxMinWidth, boundingBoxMinHeight, boundingBoxMaxWidth, boundingBoxMaxHeight);
				cont.paint(g);
			} else {
				BufferedImage im = new BufferedImage(boundingBoxMaxWidth, boundingBoxMaxHeight, BufferedImage.TYPE_INT_ARGB);
				cont.paint(im.getGraphics());
				ImageIO.write(im, "PNG", new File(fileLoc));

				if(exportType.equals(this.PDF_TYPE)) {
					FileOutputStream fileOut = null;
					try {
						Image image1 = Image.getInstance(fileLoc);
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

						int pages = (int) Math.ceil((double)im.getHeight() / this.MAX_DIM);
						System.out.println(im.getHeight());
						System.out.println(im.getHeight()/this.MAX_DIM);
						System.out.println(Math.ceil(im.getHeight() / this.MAX_DIM));
						if(pages == 0)
							pages = 1;
						for(int i = 0; i < pages; i++) {
							BufferedImage temp;
							if(i < pages-1) {
								temp = im.getSubimage(0, i*(int)this.MAX_DIM, im.getWidth(), (int)this.MAX_DIM);
							} else {
								temp = im.getSubimage(0, i*(int)this.MAX_DIM, im.getWidth(), im.getHeight() % (int)this.MAX_DIM);
							}
							File tempFile = new File(i+this.LOW_QUALITY_IMAGE_TYPE);
							ImageIO.write(temp, this.LOW_QUALITY_IMAGE_TYPE, tempFile);
							Image croppedImage = Image.getInstance(i+this.LOW_QUALITY_IMAGE_TYPE);
							document.add(croppedImage);
							tempFile.delete();
							
							if(i < pages-1) {
								document.newPage();
							}
						}
						document.close();

						File f = new File(fileLoc);
						f.delete();
					}catch(RuntimeException e) {
						e.printStackTrace();
					} catch (DocumentException e) {
						// TODO Auto-generated catch block
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
			} // end else
			
			if(exportType.equals(this.PDF_TYPE)) {
				Utility.showMessage("Export successful: " + fileLoc.replace(this.LOW_QUALITY_IMAGE_TYPE.toLowerCase(), this.PDF_TYPE.toLowerCase()));
			} else {
				Utility.showMessage("Export successful: " + fileLoc);
			}
		} catch (IOException e) {
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
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {
	}
}
