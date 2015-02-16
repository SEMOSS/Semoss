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
