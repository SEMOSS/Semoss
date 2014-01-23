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
package prerna.ui.components;

import java.awt.Component;
import java.awt.Graphics;

import javax.swing.ImageIcon;

/**
 * This class creates the DBCM image icon.
 */
public class DBCMIcon extends ImageIcon {
	
	/**
	 * Constructor for DBCMIcon.
	 * Creates an image icon from the specified file.
	 * @param fileName 	File name.
	 */
	public DBCMIcon(String fileName)
	{
		super(fileName);
	}
	/**
	 * Paints the icon.
	 * @param c		Component to be used if icon has no image observer.
	 * @param g 	The graphics context.
	 * @param x 	X-coordinate of icon's top left corner.
	 * @param y 	Y-coordinate of icon's top left corner.
	 */
	@Override
    public void paintIcon(Component c, Graphics g, int x, int y ) {
        g.drawImage(getImage(), 5, 6, c.getWidth(), c.getHeight(), c);
    }

}
