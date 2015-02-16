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

import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.Rectangle;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.SwingConstants;
import javax.swing.plaf.basic.BasicArrowButton;
import javax.swing.plaf.basic.BasicScrollBarUI;

import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * This class is used to create the scrollbar UI.
 */
public class NewScrollBarUI extends BasicScrollBarUI {
	private Image thumb;

	/**
	 * Constructor for NewScrollBarUI.
	 */
	public NewScrollBarUI() {
		try {
			String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			//Got to comment out DIHelper and user System.getProperty when using WindowBuilder for PlayPane
			//String workingDir = System.getProperty("user.dir");
			String picFileURL = workingDir+"/pictures/rect.png";
			File picFile = new File(picFileURL);
			thumb = ImageIO.read(picFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Paints the thumbnail for the scroll bar UI.
	 * @param g Graphics		Graphics for painting.
	 * @param c JComponent		Base JavaSwing component.
	 * @param thumbBounds 		Bounds of the thumbnail.
	 */
	@Override
	protected void paintThumb(Graphics g, JComponent c, Rectangle thumbBounds) {        
		g.translate(thumbBounds.x, thumbBounds.y);
		((Graphics2D) g).drawImage(thumb, 0, 0, thumbBounds.width, thumbBounds.height, null);
		g.translate( -thumbBounds.x, -thumbBounds.y );
	}

	/**
	 * Creates an increase button for the scroll bar UI.
	 * @param orientation	Orientation of button. 
	
	 * @return JButton 		Increase button. */
	@Override
	protected JButton createIncreaseButton(int orientation)
	{
		if (incrButton == null)
			incrButton = new BasicArrowButton((orientation == SwingConstants.HORIZONTAL) ? SwingConstants.EAST : SwingConstants.SOUTH);
		else
		{
			if (orientation == SwingConstants.HORIZONTAL)
				((BasicArrowButton) incrButton).setDirection(SwingConstants.EAST);
			else
				((BasicArrowButton) incrButton).setDirection(SwingConstants.SOUTH);
		}
		incrButton.setOpaque(false);
		return incrButton;
	}

	/**
	 * Creates a decrease button in the horizontal scroll bar UI.
	 * @param orientation 	Orientation of button.
	
	 * @return JButton 		Decrease button. */
	@Override
	protected JButton createDecreaseButton(int orientation)
	{
		if (decrButton == null)
			decrButton = new BasicArrowButton((orientation == SwingConstants.HORIZONTAL) ? SwingConstants.WEST : SwingConstants.NORTH);
		else
		{
			if (orientation == SwingConstants.HORIZONTAL)
				((BasicArrowButton) decrButton).setDirection(SwingConstants.WEST);
			else
				((BasicArrowButton) decrButton).setDirection(SwingConstants.NORTH);
		}
		return decrButton;
	}
}
