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
 * This class is used to paint the horizontal scrollbar UI.
 */
public class NewHoriScrollBarUI extends BasicScrollBarUI {
	private Image thumb;

	/**
	 * Constructor for NewHoriScrollBarUI.
	 */
	public NewHoriScrollBarUI() {
		try {
			String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			String picFileURL = workingDir+"/pictures/rect2.png";
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
	 * Creates an increase button for the horizontal scroll bar UI.
	 * @param orientation	Orientation of button. 
	
	 * @return JButton 		Increase button. */
	@Override
	protected JButton createIncreaseButton(int orientation)
	{
		if (incrButton == null)
			incrButton = new BasicArrowButton((orientation == SwingConstants.HORIZONTAL) ? SwingConstants.NORTH : SwingConstants.EAST);
		else
		{
			if (orientation == SwingConstants.HORIZONTAL)
				((BasicArrowButton) incrButton).setDirection(SwingConstants.NORTH);
			else
				((BasicArrowButton) incrButton).setDirection(SwingConstants.EAST);
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
			decrButton = new BasicArrowButton((orientation == SwingConstants.HORIZONTAL) ? SwingConstants.SOUTH : SwingConstants.WEST);
		else
		{
			if (orientation == SwingConstants.HORIZONTAL)
				((BasicArrowButton) decrButton).setDirection(SwingConstants.SOUTH);
			else
				((BasicArrowButton) decrButton).setDirection(SwingConstants.WEST);
		}
		return decrButton;
	}
}
