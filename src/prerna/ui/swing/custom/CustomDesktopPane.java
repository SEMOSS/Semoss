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
package prerna.ui.swing.custom;

import java.awt.Graphics;
import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

import javax.swing.ImageIcon;
import javax.swing.JDesktopPane;

/**
 * This class extends JDesktopPane in order to create a custom desktop pane.
 */
public class CustomDesktopPane extends JDesktopPane{
	String workingDir = System.getProperty("user.dir");
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
