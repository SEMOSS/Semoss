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
package prerna.ui.swing.custom;

import java.awt.Graphics;
import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

import javax.swing.ImageIcon;
import javax.swing.JDesktopPane;

import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * This class extends JDesktopPane in order to create a custom desktop pane.
 */
public class CustomDesktopPane extends JDesktopPane{
	String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
	final String fileString = workingDir +"/pictures/desktop.png";
	ImageIcon icon = new ImageIcon(fileString);
    Image image = icon.getImage();

    private BufferedImage img;
    /**
     * This constructor is used to create the desktop pane.
     */
    public CustomDesktopPane()  
    {  
    	File file = new File(fileString);
    	try {
			img = javax.imageio.ImageIO.read(file);
		} catch (IOException e) {
			e.printStackTrace();
		} 
    }  
    
    /**
     * Paints the desktop pane.
     * @param g Graphics		Graphic to be displayed.
     */
    @Override
    protected void paintComponent(Graphics g)
    {
    	super.paintComponent(g);  
        if(img != null) 
        	g.drawImage(img, 0,0,this.getWidth(),this.getHeight(),this);  
        else g.drawString("Image not found", 50,50);  
        
    }

}
