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
package prerna.ui.transformer;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;
import javax.swing.Icon;
import javax.swing.ImageIcon;

import org.apache.commons.collections15.Transformer;

import prerna.om.SEMOSSVertex;
import prerna.ui.components.DBCMIcon;
import prerna.util.Constants;
import prerna.util.DIHelper;
import edu.uci.ics.jung.visualization.decorators.DefaultVertexIconTransformer;

/**
 */
public class VertexIconTransformer extends DefaultVertexIconTransformer<SEMOSSVertex>  implements Transformer<SEMOSSVertex, Icon>{
	
	public static VertexIconTransformer tx = null;
	
	/**
	 * Constructor for VertexIconTransformer.
	 */
	public VertexIconTransformer()//Transformer<DBCMVertex,Icon> delegate)
	{
		//super(delegate);
	}
	
	 boolean fillImages = true;
     boolean outlineImages = false;

     /**
     
      * @return Returns the fillImages. */
     public boolean isFillImages() {
         return fillImages;
     }
     /**
      * @param fillImages The fillImages to set.
      */
     public void setFillImages(boolean fillImages) {
         this.fillImages = fillImages;
     }

     /**
      * Method isOutlineImages.
     
      * @return boolean */
     public boolean isOutlineImages() {
         return outlineImages;
     }
     /**
      * Method setOutlineImages.
      * @param outlineImages boolean
      */
     public void setOutlineImages(boolean outlineImages) {
         this.outlineImages = outlineImages;
     }
     
	/**
	 * Method transform.
	 * @param arg0 DBCMVertex
	
	 * @return Icon */
	@Override
	public Icon transform(SEMOSSVertex arg0) {
		// get the DI Helper to find what is the property we need to get for vertex
		// based on that get that property and return it
		
		try {
			String propType = (String)arg0.getProperty(Constants.VERTEX_TYPE);
			
			String fileName = DIHelper.getInstance().getProperty("System_ICON");
			fileName = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/pictures/globe.jpg";

			if(propType !=null && fileName != null)
			{
				DBCMIcon icon = new DBCMIcon(fileName);
				Image scaledImage = icon.getImage();
				scaledImage = scaledImage.getScaledInstance(20, 20, 0);
				
				BufferedImage image = ImageIO.read(new File(fileName));
				
				ImageIcon icon2 = new ImageIcon(scaledImage);
				
				int borderWidth = 1;
				int spaceAroundIcon = -2;
				Color borderColor = Color.RED;

				BufferedImage bi = new BufferedImage(icon.getIconWidth() + (2 * borderWidth + 2 * spaceAroundIcon),icon.getIconHeight() + (2 * borderWidth + 2 * spaceAroundIcon), BufferedImage.TYPE_INT_ARGB);

				Graphics2D g = bi.createGraphics();
				g.setColor(borderColor);
				g.drawImage(icon.getImage(), borderWidth + spaceAroundIcon, borderWidth + spaceAroundIcon, null);
				BasicStroke stroke = new BasicStroke(2); //5 pixels wide (thickness of the border)
				g.setStroke(stroke);

				//Image scaledImage2 = bi.getScaledInstance(20,20,0);
				
				g.drawOval(0, 0, bi.getWidth(), bi.getHeight());
				//g.drawOval(0, 0, 20, 20);
				g.dispose();
								
				return new ImageIcon(bi);
				
				//icon.
				//ImageIcon icon2 = icon.
				//return icon2;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
}
