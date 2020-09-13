/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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
package prerna.ui.transformer;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Shape;
import java.awt.geom.AffineTransform;
import java.awt.geom.Ellipse2D;
import java.awt.geom.Point2D;

import edu.uci.ics.jung.algorithms.layout.BalloonLayout;
import edu.uci.ics.jung.algorithms.layout.Layout;
import edu.uci.ics.jung.visualization.Layer;
import edu.uci.ics.jung.visualization.VisualizationServer;
import edu.uci.ics.jung.visualization.VisualizationViewer;
import edu.uci.ics.jung.visualization.transform.MutableTransformer;
import edu.uci.ics.jung.visualization.transform.MutableTransformerDecorator;
import prerna.om.SEMOSSVertex;

/**
 * Writes rings on the graph for the balloon layout.
 */
public class BalloonLayoutRings implements VisualizationServer.Paintable {
	Layout layout;
	VisualizationViewer vv;
	
	/**
	 * Constructor for BalloonLayoutRings.
	 */
	public BalloonLayoutRings(){
		
	}
	
	/**
	 * Method setViewer.  Sets the local visualization viewer.
	 * @param view VisualizationViewer
	 */
	public void setViewer(VisualizationViewer view){
		this.vv = view;
	}
	
	/**
	 * Method setLayout.  Sets the local graph layout.
	 * @param layout Layout
	 */
	public void setLayout(Layout layout){
		this.layout = layout;
	}
	
	/**
	 * Method paint.  Paints the rings on the graph.
	 * @param g Graphics - the graphics to be painted.
	 */
	public void paint(Graphics g) {
		g.setColor(Color.gray);
	
		Graphics2D g2d = (Graphics2D)g;

		Ellipse2D ellipse = new Ellipse2D.Double();
		for(SEMOSSVertex v : (Iterable<SEMOSSVertex>) layout.getGraph().getVertices()) {
			Double radius = (Double) ((BalloonLayout)layout).getRadii().get(v);
			if(radius == null) continue;
			Point2D p = (Point2D) layout.transform(v);
			ellipse.setFrame(-radius, -radius, 2*radius, 2*radius);
			AffineTransform at = AffineTransform.getTranslateInstance(p.getX(), p.getY());
			Shape shape = at.createTransformedShape(ellipse);
			
			MutableTransformer viewTransformer =
				vv.getRenderContext().getMultiLayerTransformer().getTransformer(Layer.VIEW);
			
			if(viewTransformer instanceof MutableTransformerDecorator) {
				shape = vv.getRenderContext().getMultiLayerTransformer().transform(shape);
			} else {
				shape = vv.getRenderContext().getMultiLayerTransformer().transform(Layer.LAYOUT,shape);
			}

			g2d.draw(shape);
		}
	}

	/**
	 * Method useTransform. Determines whether or not to use a transform.
	 * @return boolean - returns true
	 */
	public boolean useTransform() {
		return true;
	}
}
