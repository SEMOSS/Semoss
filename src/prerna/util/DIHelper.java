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
package prerna.util;



import java.awt.Color;
import java.awt.Shape;
import java.awt.geom.Ellipse2D;
import java.awt.geom.GeneralPath;
import java.awt.geom.Rectangle2D;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Properties;

import javax.swing.JList;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;

import com.ibm.icu.util.StringTokenizer;

import edu.uci.ics.jung.algorithms.layout.BalloonLayout;
import edu.uci.ics.jung.algorithms.layout.CircleLayout;
import edu.uci.ics.jung.algorithms.layout.FRLayout;
import edu.uci.ics.jung.algorithms.layout.ISOMLayout;
import edu.uci.ics.jung.algorithms.layout.KKLayout;
import edu.uci.ics.jung.algorithms.layout.RadialTreeLayout;
import edu.uci.ics.jung.algorithms.layout.SpringLayout;
import edu.uci.ics.jung.algorithms.layout.TreeLayout;

/**
 * DIHelper is used throughout SEMOSS to obtain property names from core propfiles and engine core propfiles.
 */
public class DIHelper {
	
	// helps with all of the dependency injection
	public static DIHelper helper = null;
	
	IEngine rdfEngine = null;
	Properties rdfMap = null;
	
	// core properties file
	Properties coreProp = null;
	// engine core prop file
	Properties engineCoreProp = null;
	// extended properties
	Properties extendProp = null;
	// Hashtable of local properties
	// will have the following keys
	// Perspective -<Hashtable of questions and identifier> - Possibly change this over to vector
	// Question-ID Key
	Hashtable localProp = new Hashtable();

	// localprop for engine
	Hashtable engineLocalProp = new Hashtable();
	
	// cached questions for an engine
	Hashtable <String, Hashtable> engineQHash = new Hashtable<String, Hashtable>();
	// Logger
	static final Logger logger = LogManager.getLogger(DIHelper.class.getName());
	
	/**
	 * Constructor for DIHelper.
	 */
	protected DIHelper()
	{
		// do nothing
	}
	
	/**
	 * Set up shapes, colors, and layouts. 
	 * Put properties for each in a hashtable of local properties.
	
	 * @return DIHelper 		Properties. */
	public static DIHelper getInstance()
	{
		if(helper == null)
		{
			helper = new DIHelper();
			// need to set up the shapes here
			//Shape square = new Rectangle2D.Double(-5,-5,10, 10);

			//new Graphics2D().dr
			//square = (Shape) g2;
			//Shape circle = new Ellipse2D.Double(-5, -5, 10, 10);
			Ellipse2D.Double circle = new Ellipse2D.Double(-6, -6, 12, 12);

			Rectangle2D.Double square = new Rectangle2D.Double(-6,-6,12, 12);
			//RoundRectangle2D.Double round = new RoundRectangle2D.Double(-6,-6,12, 12, 6, 6);
			
			Shape triangle = helper.createUpTriangle(6);
			Shape star = helper.createStar();
			Shape rhom = helper.createRhombus(7);
			Shape hex = helper.createHex(7);
			Shape pent = helper.createPent(7);
			

			helper.localProp.put(Constants.SQUARE, square);
			helper.localProp.put(Constants.CIRCLE, circle);
			helper.localProp.put(Constants.TRIANGLE, triangle);
			helper.localProp.put(Constants.STAR, star);
			helper.localProp.put(Constants.DIAMOND, rhom);
			helper.localProp.put(Constants.HEXAGON, hex);
			helper.localProp.put(Constants.PENTAGON, pent);

			Shape squareL = new Rectangle2D.Double(0,0,40, 40);
			//Shape circleL = new Ellipse2D.Double(0, 0, 13, 13);
			Shape circleL = new Ellipse2D.Double(0,0,20,20);
			Shape triangleL = helper.createUpTriangleL();
			Shape starL = helper.createStarL();
			Shape rhomL = helper.createRhombusL();
			Shape pentL = helper.createPentL();
			Shape hexL = helper.createHexL();
			
			helper.localProp.put(Constants.SQUARE + Constants.LEGEND, squareL);
			helper.localProp.put(Constants.CIRCLE + Constants.LEGEND, circleL);
			helper.localProp.put(Constants.TRIANGLE + Constants.LEGEND, triangleL);
			helper.localProp.put(Constants.STAR + Constants.LEGEND, starL);
			helper.localProp.put(Constants.HEXAGON + Constants.LEGEND, hex);
			helper.localProp.put(Constants.DIAMOND + Constants.LEGEND, rhomL);
			helper.localProp.put(Constants.PENTAGON + Constants.LEGEND, pentL);
			helper.localProp.put(Constants.HEXAGON + Constants.LEGEND, hexL);

			Color blue = new Color(31, 119, 180);
			Color green = new Color(44, 160, 44);
			Color red = new Color(214, 39, 40);
			Color brown = new Color(143, 99, 42);
			Color yellow = new Color(254, 208, 2);
			Color orange = new Color(255, 127, 14);
			Color purple = new Color(148, 103, 189);
			Color aqua = new Color(23, 190, 207);
			Color pink = new Color(241, 47, 158);
			
			helper.localProp.put(Constants.BLUE, blue);
			helper.localProp.put(Constants.GREEN, green);
			helper.localProp.put(Constants.RED, red);
			helper.localProp.put(Constants.BROWN, brown);
			helper.localProp.put(Constants.MAGENTA, pink);
			helper.localProp.put(Constants.YELLOW, yellow);
			helper.localProp.put(Constants.ORANGE, orange);
			helper.localProp.put(Constants.PURPLE, purple);
			helper.localProp.put(Constants.AQUA, aqua);
			
			// put all the layouts as well
			helper.localProp.put(Constants.FR, FRLayout.class);
			helper.localProp.put(Constants.KK, KKLayout.class);
			helper.localProp.put(Constants.ISO, ISOMLayout.class);
			helper.localProp.put(Constants.SPRING, SpringLayout.class);
			helper.localProp.put(Constants.CIRCLE_LAYOUT, CircleLayout.class);
			helper.localProp.put(Constants.RADIAL_TREE_LAYOUT, RadialTreeLayout.class);
			helper.localProp.put(Constants.TREE_LAYOUT, TreeLayout.class);
			helper.localProp.put(Constants.BALLOON_LAYOUT, BalloonLayout.class);

		}
		return helper; 
	}
	


	/**
	 * Obtains a specific RDF engine.
	
	 * @return IEngine 		RDF engine. */
	public IEngine getRdfEngine() {
		return rdfEngine;
	}

	/**
	 * Sets the specific RDF engine.
	 * @param IEngine		Obtained RDF engine.
	 */
	public void setRdfEngine(IEngine rdfEngine) {
		this.rdfEngine = rdfEngine;
	}
	
	
	/**
	 * Gets core properties.
	
	 * @return Properties		List of core properties. */
	public Properties getCoreProp() {
		return coreProp;
	}
	
	/**
	 * Gets engine properties.
	
	 * @return Properties 		List of engine properties. */
	public Properties getEngineProp(){
		return engineCoreProp;
	}

	/**
	 * Sets core properties from list.
	 * @param coreProp Properties		Obtained list of core properties.
	 */
	public void setCoreProp(Properties coreProp) {
		this.coreProp = coreProp;
	}

	/**
	 * Gets the RDF Map.
	
	 * @return List of properties in RDF map */
	public Properties getRdfMap() {
		return engineCoreProp;
	}

	
	/**
	 * Retrieves properties from hashtable.
	 * @param String		Key used to retrieve properties
	
	 * @return				Property name */
	public String getProperty(String name)
	{
		String retName = coreProp.getProperty(name);
		
		//if(retName == null && engineCoreProp != null && engineCoreProp.containsKey(name))
			//retName = "" + engineCoreProp.get(name);
		
		  		JList list = (JList) DIHelper.getInstance().getLocalProp(
				Constants.REPO_LIST);
		
		if(retName == null && list != null)
		{
			// get the selected repository
			Object[] repos = (Object[]) list.getSelectedValues();
			if(repos != null && repos.length > 0)
			{
				IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(repos[0] + "");
				System.err.println("Engine is " + engine.getEngineName() + name);
				retName =  engine.getProperty(name);
				System.err.println("RetName " + retName);
			}			
		}

		 
		//logger.debug("Engine Local Prop" + engineLocalProp);
		return retName;
	}

	/**
	 * Puts properties in the core property hashtable.
	 * @param name String		Hash key.
	 * @param value String		Value mapped to specific key.
	 */
	public void putProperty(String name, String value)
	{
		coreProp.put(name, value);
	}

	  /**
	   * Creates a star shape.
	  
	   * @return 	Star */
	  public Shape createStar() 
	  {
		  double x = .5;
		  double points[][] = { 
		        { 0*x, -15*x }, { 4.5*x, -5*x }, { 14.5*x,-5*x}, { 7.5*x,3*x }, 
		        { 10.5*x, 13*x}, { 0*x, 7*x }, { -10.5*x, 13*x}, { -7.5*x, 3*x }, 
		        {-14.5*x,-5*x}, { -4.5*x,-5*x}, { 0, -15*x} 
		    };
	      final GeneralPath star = new GeneralPath();
	        star.moveTo(points[0][0], points[0][1]);

	        for (int k = 1; k < points.length; k++)
	            star.lineTo(points[k][0], points[k][1]);

	      star.closePath();
	      return star;
	  }

	  /**
	   * Creates a star shape for the legend.
	  
	   * @return 	Star */
	  public Shape createStarL() 
	  {
		  //double points[][] = {{7.5,0} ,{9,5} ,{14.5, 5}, {11, 9}, {12.5, 14}, {7.2, 10.5}, {2.2, 14}, {3.5, 9}, {0,5}, {5, 5}, {7.5, 0}};
		  double points[][] = {{10,0} ,{13,6.66} ,{20, 6.66}, {14.66, 12}, {16.66, 18.66}, {10, 14}, {3.33, 18.66}, {5.33, 12}, {0,6.66}, {7, 6.66}, {10, 0}};
		  
	      final GeneralPath star = new GeneralPath();
	        star.moveTo(points[0][0], points[0][1]);

	        for (int k = 1; k < points.length; k++)
	            star.lineTo(points[k][0], points[k][1]);

	      star.closePath();
	      return star;
	  }
	  
	  /**
	   * Creates a hexagon shape.
	   * @param 		Specifies start position (X-coordinate) for drawing the hexagon.
	  
	   * @return 		Hexagon */
	  public Shape createHex(final double s) 
	  {
			GeneralPath hexagon = new GeneralPath();
			hexagon.moveTo(s, 0);
			for (int i = 0; i < 6; i++) 
			    hexagon.lineTo((float)Math.cos(i*Math.PI/3)*s, 
					   (float)Math.sin(i*Math.PI/3)*s);
			hexagon.closePath();
	      return hexagon;
	  }

	  /**
	   * Creates a hexagon shape for the legend
	  
	   * @return 		Hexagon */
	  public Shape createHexL() 
	  {
		  double points[][] = {{20,10} ,{15, 0} ,{5, 0}, {0, 10}, {5, 20}, {15,20}};
		  
	      final GeneralPath pent = new GeneralPath();
	        pent.moveTo(points[0][0], points[0][1]);

	        for (int k = 1; k < points.length; k++)
	            pent.lineTo(points[k][0], points[k][1]);

	      pent.closePath();
	      return pent;
	  }
	  
	  /**
	   * Creates a pentagon shape.
	   * @param 		Specifies start position (X-coordinate) for drawing the pentagon
	  
	   * @return 		Pentagon */
	  public Shape createPent(final double s) 
	  {
			GeneralPath hexagon = new GeneralPath();
			hexagon.moveTo((float)Math.cos(Math.PI/10)*s, (float)Math.sin(Math.PI/10)*(-s));
			for (int i = 0; i < 5; i++) 
			    hexagon.lineTo((float)Math.cos(i*2*Math.PI/5 + Math.PI/10)*s, 
					   (float)Math.sin(i*2*Math.PI/5 + Math.PI/10)*(-s));
			hexagon.closePath();
	      return hexagon;
	  }
	  
	  /**
	   * Creates a pentagon shape for the legend.
	  
	   * @return 		Pentagon */
	  public Shape createPentL() 
	  {
		  double points[][] = {{10,0} ,{19.510565163, 6.90983005625} ,{15.8778525229, 18.0901699437}, {4.12214747708, 18.0901699437}, {0.48943483704, 6.90983005625}};
		  
	      final GeneralPath pent = new GeneralPath();
	        pent.moveTo(points[0][0], points[0][1]);

	        for (int k = 1; k < points.length; k++)
	            pent.lineTo(points[k][0], points[k][1]);

	      pent.closePath();
	      return pent;
	  }
	  
	  /**
	   * Creates a rhombus shape.
	   * @param 		Specifies start position (X-coordinate) for drawing the rhombus
	  
	   * @return		Rhombus */
	  public Shape createRhombus(final double s) 
	  {
		  double points[][] = { 
			        { 0, -s }, { -s, 0}, { 0,s}, { s,0 }, 
			    };
		      final GeneralPath r = new GeneralPath();
		        r.moveTo(points[0][0], points[0][1]);

		        for (int k = 1; k < points.length; k++)
		            r.lineTo(points[k][0], points[k][1]);

		      r.closePath();
		      return r;
	  }

	  /**
	   * Creates a rhombus shape for the legend.
	  
	   * @return 	Rhombus */
	  public Shape createRhombusL() 
	  {
		  double points2[][] = { 
			        { 10, 0 }, { 0, 10}, { 10,20}, { 20,10 }, 
			    };
	      final GeneralPath r = new GeneralPath(); // rhombus
	        r.moveTo(points2[0][0], points2[0][1]);

	        for (int k = 1; k < points2.length; k++)
	            r.lineTo(points2[k][0], points2[k][1]);

	      r.closePath();
	      return r;
	  }

	  /**
	   * Creates a triangle.
	   * @param 		Specifies start position (X-coordinate) for drawing the triangle.
	  
	   * @return 		Triangle */
	  public Shape createUpTriangle(final double s) 
	  {
	      final GeneralPath p0 = new GeneralPath();
	      p0.moveTo(0, -s);
	      p0.lineTo(s, s);
	      p0.lineTo(-s, s);
	      p0.closePath();
	      return p0;
	  }

	  /**
	   * Creates a triangle for the legend.
	  
	   * @return 	Triangle */
	  public Shape createUpTriangleL() 
	  {
	      GeneralPath p0 = new GeneralPath(); // triangle
	      
	      p0.moveTo(10, 0);
	      p0.lineTo(20, 20);
	      p0.lineTo(0, 20);
	      p0.closePath();
	      return p0;
	  }

	  /**
	   * Loads engine specific properties.
	   * @param String				Name of the engine
	   * @param String				Prop file
	   * @param String				Prop file containing ontologies
	   */
	  public void loadEngineProp(String engineName, String qPropFile, String ontologyPropFile)
	  {
		  FileInputStream qPropfileIn = null;
		  FileInputStream ontologyPropfileIn = null;
			try {
				if(!engineQHash.containsKey(engineName))
				{
					engineCoreProp = new Properties();
					qPropfileIn = new FileInputStream(qPropFile);
					engineCoreProp.load(qPropfileIn);
					Hashtable engineLocalProp = new Hashtable();
					loadPerspectives(engineCoreProp, engineLocalProp);
					engineQHash.put(engineName, engineLocalProp);
					ontologyPropfileIn = new FileInputStream(ontologyPropFile); 
					engineCoreProp.load(ontologyPropfileIn);
					engineQHash.put(engineName + "_CORE_PROP", engineCoreProp);
				}
				engineCoreProp = (Properties)engineQHash.get(engineName + "_CORE_PROP");
				engineLocalProp = engineQHash.get(engineName);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}finally{
				try{
					if(qPropfileIn!=null)
						qPropfileIn.close();

				}catch(IOException e) {
					e.printStackTrace();
				}
				try{
					if(ontologyPropfileIn!=null)
						ontologyPropfileIn.close();
				}catch(IOException e) {
					e.printStackTrace();
				}
			}

	  }
	  
	  /**
	   * Load the perspectives for a specific engine.
	   * @param 	List of properties
	   * @param  	Hashtable of local engine properties
	   */
	  public void loadPerspectives(Properties prop, Hashtable engineLocalProp)
	  {
		  // this should load the properties from the specified as opposed to 
		  // loading from core prop
		  // lastly the localprop needs to set up so that it can be swapped
		  String perspectives = (String)prop.get(Constants.PERSPECTIVE);
		  StringTokenizer tokens = new StringTokenizer(perspectives, ";");
		  Hashtable perspectiveHash = new Hashtable();
		  while(tokens.hasMoreTokens())
		  {
			  String perspective = tokens.nextToken();
			  perspectiveHash.put(perspective, perspective); // the value will be replaced later with another hash
			  engineLocalProp.put(Constants.PERSPECTIVE, perspectiveHash);
			  loadQuestions(prop, perspective, engineLocalProp);			  	
		  }
	  }
	  
	  /**
	   * Retrieves local properties from the hashtable. 
	   * @param 	Key to the hashtable
	  
	   * @return 	Property mapped to a specific key */
	  public Object getLocalProp(String key)
	  {
		  if(localProp.containsKey(key))
			  return localProp.get(key);
		  else
			  return engineLocalProp.get(key);
	  }

	  /**
	   * Loads questions for a particular perspective.
	   * @param 	List of properties
	   * @param 	String with the name of the perspective
	   * @param 	Hashtables containing local properties of a specific engine
	   */
	  public void loadQuestions(Properties prop, String perspective, Hashtable engineLocalProp)
	  {
			String key = perspective;
			String qsList = prop.getProperty(key); // get the ; delimited questions
			
			Hashtable qsHash = new Hashtable();
			Hashtable layoutHash = new Hashtable();
			if(qsList != null)
			{
				int count = 1;
				StringTokenizer qsTokens = new StringTokenizer(qsList,";");
				while(qsTokens.hasMoreElements())
				{
					String qsKey = qsTokens.nextToken();
					String qsDescr = prop.getProperty(qsKey);
					qsDescr = count+". " + qsDescr;
					String layoutName = prop.getProperty(qsKey + "_" + Constants.LAYOUT);
					logger.info("Putting information " + qsDescr + "<>" + qsKey);
					qsHash.put(qsDescr, qsKey); // I do this because I will use this to look up other things later
					layoutHash.put(qsKey + "_" + Constants.LAYOUT, layoutName);
					engineLocalProp.put(qsDescr, qsKey);
					count++;
				}
				logger.info("Loaded Perspective " + key);
				engineLocalProp.put(key, qsHash); // replaces the previous hash with the new one now
				engineLocalProp.put(key + "_" + Constants.LAYOUT, layoutHash);
			}
	  }
	  
	  /**
	   * Gets the list of questions for a particular perspective.
	   * @param 	String containing the perspective name
	  
	   * @return 	Hashtable containing questions for a given perspective */
	  public Hashtable getQuestions(String perspective)
	  {
		  logger.info("Getting questions for perspective " + perspective);
		  logger.info("Answer " + engineLocalProp.get(perspective));
		  return (Hashtable)engineLocalProp.get(perspective);
		  
	  }
	  
	  /**
	   * Puts local properties into a hashtable.
	   * @param 	Property, serves as the hashtable key.
	   * @param 	Identifier for a given property.
	   */
	  public void setLocalProperty(String property, Object value)
	  {
		  localProp.put(property, value);
	  }
	  
	  /**
	   * Get the ID for a specific question.
	   * @param String		Question.
	  
	   * @return String		ID. */
	  public String getIDForQuestion(String question)
	  {
		  return (String)engineLocalProp.get(question);
	  }
	  
	  /**
	   * Creates a new list and loads properties given a certain file name.
	   * @param String		File name.
	   */
	  public void loadCoreProp(String fileName)
	  {
		  FileInputStream fileIn = null;
			try {
				coreProp = new Properties();
				fileIn = new FileInputStream(fileName);
				coreProp.load(fileIn);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}finally{
				try{
					if(fileIn!=null)
						fileIn.close();
				}catch(IOException e) {
					e.printStackTrace();
				}
			}
	  }
	  
	  
	  /**
	   * Gets the local prop of the engine
	   */
	  public Hashtable getEngineLocalProp(String engineName)
	  {
		  return engineQHash.get(engineName);
	  }

	  /**
	   * Gets the core prop of the engine
	   */
	  public Properties getEngineCoreProp(String engineName)
	  {
		  return (Properties) engineQHash.get(engineName + "_CORE_PROP");
	  }

	  /*
	  public Hashtable<String, DBCMVertex> getVertexStore()
	  {
		  return this.vertHash;
	  }
	  
	  public Hashtable<String, DBCMEdge> getEdgeStore()
	  {
		  return this.edgeHash;
	  }
	  */
	  
}
