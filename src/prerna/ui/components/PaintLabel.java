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
package prerna.ui.components;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Shape;

import javax.swing.JLabel;

/**
 * This class is used to paint a text string on a display area.
 */
public class PaintLabel extends JLabel {

	Shape shape = null;
	Color color = null;
	
	/**
	 * Constructor for PaintLabel.
	 * @param text 	Text string to be painted.
	 */
	public PaintLabel(String text)
	{
		super(text);
	}
	
	/**
	 * Calls the UI delegate's paint method.
	 * @param g 	Graphics object to protect.
	 */
	@Override
	public void paintComponent(Graphics g)
	{
		if(shape != null && color != null)
		{
			//super(g);
			Graphics2D dg2 = (Graphics2D)g;
			dg2.setColor(color);
			dg2.fill(shape);
		}
	}
	
	/**
	 * Sets the shape of the display area.
	 * @param shape Shape.
	 */
	public void setShape(Shape shape)
	{
		this.shape = shape;
	}
	
	/**
	 * Sets the color of the display area.
	 * @param color 	Color to be displayed.
	 */
	public void setColor(Color color)
	{
		this.color = color;
	}
}
/*
 * 			//dg2.fil
			Shape square = new Rectangle2D.Double(0,0,40, 40);
			square = new Ellipse2D.Double(0, 0, 15, 15);
			
			/*  double points[][] = { 
				        { 0, -15 }, { 4.5, -5 }, { 14.5,-5}, { 7.5,3 }, 
				        { 10.5, 13}, { 0, 7 }, { -10.5, 13}, { -7.5, 3 }, 
				        {-14.5,-5}, { -4.5,-5}, { 0, -15} 
				    };
			  
			 /*y =  -15 to 7 = Add 15
			 x = -14.5 to 14.5  = add 14.5
			  
			  //double points[][] = {{14.5,0} ,{19,10} ,{29, 10}, {22, 18}, {25, 28}, {14.5, 21}, {4.5, 28}, {7, 18}, {0,10}, {10, 10}, {14.5, 0}};
			  double points[][] = {{7.5,0} ,{9,5} ,{14.5, 5}, {11, 9}, {12.5, 14}, {7.2, 10.5}, {2.2, 14}, {3.5, 9}, {0,5}, {5, 5}, {7.5, 0}};
			  
			      final GeneralPath star = new GeneralPath();
			        star.moveTo(points[0][0], points[0][1]);

			        for (int k = 1; k < points.length; k++)
			            star.lineTo(points[k][0], points[k][1]);

			      star.closePath();
			      square = star;

			      
			      
	      GeneralPath p0 = new GeneralPath(); // triangle
			      
	      p0.moveTo(20, 0);
	      p0.lineTo(10, 20);
	      p0.lineTo(0, 0);
	      p0.closePath();
	      
	      		  /*p0.moveTo(10, 5);
			      p0.lineTo(5, 10);
			      p0.lineTo(0, 5);
			      p0.closePath();
			      square = p0;
			      
				  double points2[][] = { 
					        { 10, 0 }, { 0, 10}, { 10,20}, { 20,10 }, 
					    };
			      final GeneralPath r = new GeneralPath(); // rhombus
			        r.moveTo(points2[0][0], points2[0][1]);

			        for (int k = 1; k < points2.length; k++)
			            r.lineTo(points2[k][0], points2[k][1]);

			      r.closePath();
			      
			      square = p0;

			//dg2.scale(-1, -1);

*/
