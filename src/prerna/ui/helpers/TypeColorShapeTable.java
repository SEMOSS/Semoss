package prerna.ui.helpers;

import java.awt.Color;
import java.awt.Shape;
import java.util.Hashtable;

import org.apache.log4j.Logger;

import prerna.util.Constants;
import prerna.util.DIHelper;

public class TypeColorShapeTable {
	
	// this is the table that keeps all the shapes for various types of objects
	// has 2 hash tables one for type
	// the other for color
	
	Hashtable <String, Shape> shapeHash = new Hashtable<String, Shape>();
	Hashtable <String, Color> colorHash = new Hashtable<String, Color>();

	Hashtable <String, Shape> shapeHashL = new Hashtable<String, Shape>();

	Hashtable <String, String> shapeStringHash = new Hashtable<String, String>();
	Hashtable <String, String> colorStringHash = new Hashtable<String, String>();

	String [] shapes = null;
	String [] colors = null;
	static TypeColorShapeTable instance = null;
	
	Logger logger = Logger.getLogger(getClass());
	
	protected TypeColorShapeTable()
	{
		
	}
	
	public static TypeColorShapeTable getInstance()
	{
		if(instance == null)
		{
			instance = new TypeColorShapeTable();
		}
		return instance;
	}
	
	public void clearAll()
	{
		shapeHash.clear();
		shapeHashL.clear();
		colorHash.clear();
		shapeStringHash.clear();
		colorStringHash.clear();
		shapeHash.put(Constants.DEFAULT_SHAPE, (Shape)DIHelper.getInstance().getLocalProp(shapes[3]));
		shapeHashL.put(Constants.DEFAULT_SHAPE + Constants.LEGEND, (Shape)DIHelper.getInstance().getLocalProp(shapes[3]+Constants.LEGEND));
		colorHash.put(Constants.DEFAULT_COLOR, (Color)DIHelper.getInstance().getLocalProp(colors[2]));
	}
	
	public String [] getAllShapes()
	{
		if(shapes == null)
		{
			shapes = new String[7];
			shapes[0] = Constants.TRIANGLE;
			shapes[1] = Constants.CIRCLE;
			shapes[2] = Constants.SQUARE;
			shapes[3] = Constants.DIAMOND;
			shapes[4] = Constants.STAR;
			shapes[6] = Constants.HEXAGON;
			shapes[5] = Constants.PENTAGON;
			shapeHash.put(Constants.DEFAULT_SHAPE, (Shape)DIHelper.getInstance().getLocalProp(shapes[3]));
			shapeHashL.put(Constants.DEFAULT_SHAPE + Constants.LEGEND, (Shape)DIHelper.getInstance().getLocalProp(shapes[3]+Constants.LEGEND));
		}
		return shapes;
	}

	public String [] getAllColors()
	{
		if(colors == null)
		{
			colors = new String[10];
			colors[0] = Constants.BLUE;
			colors[1] = Constants.GREEN;
			colors[2] = Constants.RED;
			colors[3] = Constants.BROWN;
			colors[4] = Constants.MAGENTA;
			colors[5] = Constants.ORANGE;
			colors[6] = Constants.YELLOW;
			colors[7] = Constants.AQUA;
			colors[8] = Constants.PURPLE;
			colors[9] = Constants.TRANSPARENT;
			
			colorHash.put(Constants.DEFAULT_COLOR, (Color)DIHelper.getInstance().getLocalProp(colors[2]));
		}
		return colors;
	}

	
	public void addShape(String type, String shape)
	{
		// get the shape from DIHelper
		Shape thisShape = (Shape)DIHelper.getInstance().getLocalProp(shape);
		Shape thisShapeL = (Shape)DIHelper.getInstance().getLocalProp(shape+Constants.LEGEND);
		shapeStringHash.put(type, shape);
		shapeHash.put(type, thisShape);
		shapeHashL.put(type, thisShapeL);
	}
	
	public Color getColor(String name)
	{
		return colorHash.get(name);
	}
	
	public void addColor(String type, String color)
	{
		// get the shape from DIHelper
		if(DIHelper.getInstance().getLocalProp(color) != null)
		{
			Color thisShape = (Color)DIHelper.getInstance().getLocalProp(color);
			colorHash.put(type, thisShape);
		}
		colorStringHash.put(type, color);
	}

	public Shape getShape(String type, String vertName)
	{
		logger.debug("Getting type / vertex name " + type + " <> " + vertName);
		Shape retShape = null;
		// first check the vertex name
		if(shapeHash.containsKey(vertName))
			retShape = shapeHash.get(vertName);
		else if(shapeHash.containsKey(type))
			retShape = shapeHash.get(type);
		else if(DIHelper.getInstance().getProperty(type+"_SHAPE") != null)
			// try to search the properties file for the first time
			retShape = (Shape)DIHelper.getInstance().getLocalProp(DIHelper.getInstance().getProperty(type+"_SHAPE"));
		if(retShape == null)
			retShape = shapeHash.get(Constants.DEFAULT_SHAPE);
		//logger.info("Shape for Type " + type + "[][]" + retShape);
		//shapeHash.put(type, retShape);
		return retShape;
	}
	
	public Shape getShapeL(String type, String vertName)
	{
		logger.debug("Getting type / vertex name " + type + " <> " + vertName);
		Shape retShape = null;
		// first check the vertex name
		if(shapeHashL.containsKey(vertName))
			retShape = shapeHashL.get(vertName);
		else if(shapeHashL.containsKey(type))
			retShape = shapeHashL.get(type);
		else if(DIHelper.getInstance().getProperty(type+"_SHAPE") != null)
			// try to search the properties file for the first time
			retShape = (Shape)DIHelper.getInstance().getLocalProp(DIHelper.getInstance().getProperty(type+"_SHAPE") + Constants.LEGEND);
		if(retShape == null)
			retShape = shapeHashL.get(Constants.DEFAULT_SHAPE+Constants.LEGEND);
		//logger.info("Shape for Type " + type + "[][]" + retShape);
		//shapeHash.put(type, retShape);
		return retShape;
		
	}
	
	public Color getColor(String type, String vertName)
	{
		Color retColor = null;
		// first check the vertex name
		if(colorHash.containsKey(vertName))
			retColor = colorHash.get(vertName);
		else if(colorHash.containsKey(type))
			retColor = colorHash.get(type);
		else if(DIHelper.getInstance().getProperty(type+"_COLOR") != null)
			// try to search the properties file for the first time
			retColor = (Color)DIHelper.getInstance().getLocalProp(DIHelper.getInstance().getProperty(type+"_COLOR"));
		if(retColor == null && !colorStringHash.containsKey(vertName))
			retColor = colorHash.get(Constants.DEFAULT_COLOR);
		if(colorStringHash.containsKey(vertName) && colorStringHash.get(vertName).equalsIgnoreCase(Constants.TRANSPARENT))
			retColor = null;
		//colorHash.put(type, retColor);
		return retColor;
	}
	
	public String getShapeAsString(String vertName)
	{
		if(shapeStringHash.containsKey(vertName))
			return shapeStringHash.get(vertName);
		else
			return "TBD";
	}
	public String getColorAsString(String vertName)
	{
		if(colorStringHash.containsKey(vertName))
			return colorStringHash.get(vertName);
		else
			return "TBD";
	}

}
