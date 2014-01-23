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
package prerna.ui.transformer;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Shape;
import java.awt.geom.AffineTransform;
import java.awt.geom.Ellipse2D;
import java.awt.geom.Point2D;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import prerna.om.SEMOSSVertex;
import edu.uci.ics.jung.algorithms.layout.Layout;
import edu.uci.ics.jung.algorithms.layout.PolarPoint;
import edu.uci.ics.jung.algorithms.layout.RadialTreeLayout;
import edu.uci.ics.jung.graph.Forest;
import edu.uci.ics.jung.visualization.Layer;
import edu.uci.ics.jung.visualization.VisualizationServer;
import edu.uci.ics.jung.visualization.VisualizationViewer;
import edu.uci.ics.jung.visualization.transform.MutableTransformer;
import edu.uci.ics.jung.visualization.transform.MutableTransformerDecorator;

/**
 * Draws the rings on the graph for the radial tree graph layout.
 */
public class RadialTreeLayoutRings implements VisualizationServer.Paintable {
	
	RadialTreeLayout radialLayout;
	Forest graph;
	VisualizationViewer vv;
	Collection<Double> depths;
	
	/**
	 * Constructor for RadialTreeLayoutRings.
	 */
	public RadialTreeLayoutRings() {
	}

	/**
	 * Method setViewer.  Sets the viewer to the current visualization viewer.
	 * @param view VisualizationViewer - the view that this is set to.
	 */
	public void setViewer(VisualizationViewer view){
		this.vv = view;
	}
	
	/**
	 * Method setForest. - Sets the graph forest.
	 * @param forest Forest - the forest that this is set to
	 */
	public void setForest(Forest forest){
		this.graph = forest;
	}
	
	/**
	 * Method setLayout. - Sets the type of layout, casts to radial tree layout
	 * @param layout Layout - the type of layout this is set to
	 */
	public void setLayout(Layout layout){
		this.radialLayout = (RadialTreeLayout) layout;
	}
	
	/**
	 * Method getDepths. - gets the radii for each of the rings in the graph
	
	 * @return Collection<Double> - returns the radii results.*/
	public Collection<Double> getDepths() {
		depths = new HashSet<Double>();
		Map<String,PolarPoint> polarLocations = radialLayout.getPolarLocations();
		for(SEMOSSVertex v : (Iterable<SEMOSSVertex>) graph.getVertices()) {
			PolarPoint pp = polarLocations.get(v);
			depths.add(pp.getRadius());
		}
		return depths;
	}

	/**
	 * Method paint.  Paints the rings on the graph.
	 * @param g Graphics - the graphics to be painted.
	 */
	public void paint(Graphics g) {
		getDepths();
		g.setColor(Color.gray);
		Graphics2D g2d = (Graphics2D)g;
		Point2D center = radialLayout.getCenter();

		Ellipse2D ellipse = new Ellipse2D.Double();
		for(double d : depths) {
			ellipse.setFrameFromDiagonal(center.getX()-d, center.getY()-d, 
					center.getX()+d, center.getY()+d);
			AffineTransform at = AffineTransform.getTranslateInstance(0, 0);
			Shape shape = at.createTransformedShape(ellipse);
			//Shape shape = 
			//	vv.getRenderContext().getMultiLayerTransformer().transform(ellipse);
//				vv.getRenderContext().getMultiLayerTransformer().getTransformer(Layer.LAYOUT).transform(ellipse);

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
