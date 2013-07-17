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

import org.apache.commons.collections15.Transformer;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;

import com.ibm.icu.util.StringTokenizer;

import edu.uci.ics.jung.algorithms.layout.CircleLayout;
import edu.uci.ics.jung.algorithms.layout.FRLayout;
import edu.uci.ics.jung.algorithms.layout.ISOMLayout;
import edu.uci.ics.jung.algorithms.layout.KKLayout;
import edu.uci.ics.jung.algorithms.layout.RadialTreeLayout;
import edu.uci.ics.jung.algorithms.layout.SpringLayout;
import edu.uci.ics.jung.algorithms.layout.TreeLayout;

public class DIHelper {
	
	// helps with all of the dependency injection
	public static DIHelper helper = null;
	
	IEngine rdfEngine = null;
	Properties rdfMap = null;
	//Hashtable<String, DBCMVertex> vertHash = new Hashtable<String, DBCMVertex>();
	//Hashtable<String, DBCMEdge> edgeHash = new Hashtable<String, DBCMEdge>();
	
	// all the transformers
	Transformer vertexLabelTransformer = null;
	Transformer vertexIconTransformer = null;
	Transformer vertexShapeTransformer = null;
	Transformer edgeLabelTransformer = null;
	Transformer vertexToolTipTransformer = null;
	
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
	Logger logger = Logger.getLogger(getClass());
	
	protected DIHelper()
	{
		// do nothing
	}
	
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
			Shape circleL = new Ellipse2D.Double(0, 0, 13, 13);
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

		}
		return helper; 
	}
	


	public IEngine getRdfEngine() {
		return rdfEngine;
	}

	public void setRdfEngine(IEngine rdfEngine) {
		this.rdfEngine = rdfEngine;
	}
	
	
	public Properties getCoreProp() {
		return coreProp;
	}
	
	public Properties getEngineProp(){
		return engineCoreProp;
	}

	public void setCoreProp(Properties coreProp) {
		this.coreProp = coreProp;
	}

	public Properties getRdfMap() {
		return engineCoreProp;
	}

	
	public String getProperty(String name)
	{
		String retName = coreProp.getProperty(name);
		if(retName == null && engineCoreProp != null && engineCoreProp.containsKey(name))
			retName = "" + engineCoreProp.get(name);
		//System.out.println("Engine Local Prop" + engineLocalProp);
		return retName;
	}

	public void putProperty(String name, String value)
	{
		coreProp.put(name, value);
	}

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

	  public Shape createStarL() 
	  {
		  double points[][] = {{7.5,0} ,{9,5} ,{14.5, 5}, {11, 9}, {12.5, 14}, {7.2, 10.5}, {2.2, 14}, {3.5, 9}, {0,5}, {5, 5}, {7.5, 0}};
		  
	      final GeneralPath star = new GeneralPath();
	        star.moveTo(points[0][0], points[0][1]);

	        for (int k = 1; k < points.length; k++)
	            star.lineTo(points[k][0], points[k][1]);

	      star.closePath();
	      return star;
	  }
	  
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
	  
	  public Shape createPent(final double s) 
	  {
			GeneralPath hexagon = new GeneralPath();
			hexagon.moveTo(s, 0);
			for (int i = 0; i < 5; i++) 
			    hexagon.lineTo((float)Math.cos(i*2*Math.PI/5 + Math.PI/10)*s, 
					   (float)Math.sin(i*2*Math.PI/5 + Math.PI/10)*(-s));
			hexagon.closePath();
	      return hexagon;
	  }
	  
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

	  public Shape createUpTriangle(final double s) 
	  {
	      final GeneralPath p0 = new GeneralPath();
	      p0.moveTo(0, -s);
	      p0.lineTo(s, s);
	      p0.lineTo(-s, s);
	      p0.closePath();
	      return p0;
	  }

	  public Shape createUpTriangleL() 
	  {
	      GeneralPath p0 = new GeneralPath(); // triangle
	      
	      p0.moveTo(10, 0);
	      p0.lineTo(20, 20);
	      p0.lineTo(0, 20);
	      p0.closePath();
	      return p0;
	  }

	  public void loadEngineProp(String engineName, String qPropFile, String ontologyPropFile)
	  {
		  // this will load all the engine specific stuff
			try {
				if(!engineQHash.containsKey(engineName))
				{
					engineCoreProp = new Properties();
					engineCoreProp.load(new FileInputStream(qPropFile));
					Hashtable engineLocalProp = new Hashtable();
					loadPerspectives(engineCoreProp, engineLocalProp);
					engineQHash.put(engineName, engineLocalProp);
					engineCoreProp.load(new FileInputStream(ontologyPropFile));
					engineQHash.put(engineName + "_CORE_PROP", engineCoreProp);
				}
				engineCoreProp = (Properties)engineQHash.get(engineName + "_CORE_PROP");
				engineLocalProp = engineQHash.get(engineName);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

	  }
	  
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
	  
	  public Object getLocalProp(String key)
	  {
		  if(localProp.containsKey(key))
			  return localProp.get(key);
		  else
			  return engineLocalProp.get(key);
	  }

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
	  
	  public Hashtable getQuestions(String perspective)
	  {
		  logger.info("Getting questions for perspective " + perspective);
		  logger.info("Answer " + engineLocalProp.get(perspective));
		  return (Hashtable)engineLocalProp.get(perspective);
		  
	  }
	  
	  public void setLocalProperty(String property, Object value)
	  {
		  localProp.put(property, value);
	  }
	  
	  public void setCoreProperty(String property, Object value)
	  {
		  coreProp.put(property, value);
	  }
	  
	  public String getIDForQuestion(String question)
	  {
		  return (String)engineLocalProp.get(question);
	  }
	  
	  public void loadCoreProp(String fileName)
	  {
			try {
				coreProp = new Properties();
				coreProp.load(new FileInputStream(fileName));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
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
