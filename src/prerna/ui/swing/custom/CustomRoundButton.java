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

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Shape;
import java.awt.geom.Ellipse2D;

import javax.swing.Icon;
import javax.swing.JButton;

/**
 * This class extends JButton in order to create a custom round button.
 */
public class CustomRoundButton extends JButton {

	/**
	 * Creates a button with an icon.
	 * @param icon Icon		Icon displayed on the button.
	 */
	public CustomRoundButton(Icon icon) {
		super(icon);
		Dimension size = getPreferredSize();
		size.width = size.height = Math.max(size.width, size.height);
		setPreferredSize(size);
		setContentAreaFilled(false);
	}
	
	/**
	 * Creates a button with text.
	 * @param label String		Label for the button.
	 */
	public CustomRoundButton(String label) {
		super(label);
		Dimension size = getPreferredSize();
		size.width = size.height = Math.max(size.width, size.height);
		setPreferredSize(size);
		setContentAreaFilled(false);
	}
	
	/**
	 * Paints the button.
	 * @param g Graphics		Graphic to be displayed.
	 */
	public void paintComponent(Graphics g) {
		g.setColor(Color.WHITE);
		g.fillOval(0, 0, getSize().width - 1, getSize().height - 1);
		super.paintComponent(g);
	}
	
	/*
	public void paintBorder(Graphics g) {
		g.setColor(Color.BLACK);
		g.drawOval(0, 0, getSize().width - 1, getSize().height - 1);
	}
	*/
	
	Shape shape;

	/**
	 * Defines the precise shape of the button.
	 * @param x int			X-coordinate of the point.
	 * @param y int			Y-coordinate of the point.
	
	 * @return boolean 		True if the component contains the point (x,y) */
	public boolean contains(int x, int y) {
		if (shape == null || !shape.getBounds().equals(getBounds())) {
			shape = new Ellipse2D.Float(0, 0, getWidth(), getHeight());
		}
		return shape.contains(x, y);
	}
}
