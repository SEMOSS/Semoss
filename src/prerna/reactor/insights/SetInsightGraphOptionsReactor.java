package prerna.reactor.insights;

import java.awt.Color;
import java.awt.Shape;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.ui.helpers.TypeColorShapeTable;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class SetInsightGraphOptionsReactor extends AbstractReactor{
	
	//SetInsightGraphOptions( options=[ "GRAPH_COLORS":{"columnname":"color value", ...}, "GRAPH_SHAPES":{...} ]);
	//SetInsightGraphOptions ( options = [ { "GRAPH_COLORS" : { "System" : "BLUE" , "Interface" : "Yellow" } , "GRAPH_SHAPES" : { "System" : "Triangle" , "Interface" : "CIRCLE" } } ] ) ;
	//SetInsightGraphOptions ( options = [ { "GRAPH_COLORS" : { "System" : "BLUE" , "Interface" : "Yellow" } } ] ) ;
	//SetInsightGraphOptions ( options = [ { "GRAPH_COLORS" : { "System" : BLUE , "Interface" : Yellow } } ] ) ;
	public SetInsightGraphOptionsReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.OPTIONS.getKey()};
		this.keyRequired = new int[] {1};
	}
	
	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		// get the Array of Objects set by the user
		GenRowStruct grs = this.getNounStore().getNoun(ReactorKeysEnum.OPTIONS.getKey());
		if(grs != null) {
			
			List<Object> values = grs.getAllValues();
			for(Object value : values) {
				Map<String, Object> mapValue = (Map<String, Object>) value;
				for(String key : mapValue.keySet()) {
					Map<String, String> items = (Map<String, String>) mapValue.get(key);
					switch (key) {
						case Constants.GRAPH_COLORS:
							Map<String,Color> graphColors = items.entrySet().stream().collect(Collectors.toMap(
					                e1 -> e1.getKey(),
					                e1 -> getColor(e1.getValue())));
							this.insight.getVarStore().put(key, new NounMetadata(graphColors, PixelDataType.MAP));
							break;
						case Constants.GRAPH_SHAPES:
							Map<String,Shape> graphShapes = items.entrySet().stream().collect(Collectors.toMap(
					                e1 -> e1.getKey(),
					                e1 -> getShape(e1.getValue())));  
							this.insight.getVarStore().put(key, new NounMetadata(graphShapes, PixelDataType.MAP));
							break;
						default:
							throw new IllegalArgumentException("Option has not yet been defined/implemented");
					}
				}
				return new NounMetadata(mapValue, PixelDataType.MAP, PixelOperationType.FORCE_SAVE_DATA_TRANSFORMATION);
			}
		}
		return new NounMetadata(Collections.<String, String>emptyMap(), PixelDataType.MAP, PixelOperationType.FORCE_SAVE_DATA_TRANSFORMATION);
	}
	
	public Color getColor(String userDefinedColor) {
		Color retColor = null;
		String nodeColor = userDefinedColor.toUpperCase();
		// check if it is a predefined color
		retColor = (Color) DIHelper.getInstance().getLocalProp(nodeColor.toUpperCase());
		
		// if reColor is not null, then we have a match. Otherwise, need try other options
		if (retColor == null ) {
			// check if its an hex code 
			if (nodeColor.contains("#")) {
				retColor = Color.decode(nodeColor);
			} 
			// check if its an rgb
			else if (nodeColor.contains("RGB")) {
				retColor = parseRgb(nodeColor);
			}
				// if its a word then try reflection to get it
			else {
				try {
				    Field field = Class.forName("java.awt.Color").getField(nodeColor.toLowerCase());
				    retColor = (Color) field.get(null);
				} catch (Exception e) {
					retColor = null; // Not defined
				}
			}
		}
		return retColor;
	}
	
	/**
	 * Method parseRgb. - Translates RGB colors defined in a string into an actual RGB Color object.
	 * @param rgbString String - rgb string - the appropriate rgb string the method will try to convert
	
	*/
	public static Color parseRgb(String rgbString) {
		Pattern c = Pattern.compile("RGB *\\( *([0-9]+), *([0-9]+), *([0-9]+) *\\)");
	    Matcher m = c.matcher(rgbString);
	    if (m.matches()) 
	    {
	        return new Color(Integer.valueOf(m.group(1)),  // r
	                         Integer.valueOf(m.group(2)),  // g
	                         Integer.valueOf(m.group(3))); // b 
	    }
	    return null;  
	}
	
	/**
	 * Method getShape.  Gets the shape of a node based on the parameters
	 * @param type String - the type of the node
	 * @param vertName String - the name of the vertex
	
	 * @return Shape - the shape of the node*/
	public Shape getShape(String type) {
		Shape retShape = null;
		String nodeShape = type.toUpperCase();
		retShape = (Shape)DIHelper.getInstance().getLocalProp(type.toUpperCase());
		// first check the vertex name
		if(DIHelper.getInstance().getProperty(type+"_SHAPE") != null)
			retShape = (Shape)DIHelper.getInstance().getLocalProp(DIHelper.getInstance().getProperty(type+"_SHAPE"));
		if(retShape == null){
			//if we couldnt find a shape, just grab a random color
			String [] keys = TypeColorShapeTable.getInstance().getAllShapes();
			String shapeString = keys[new Random().nextInt(keys.length)];
			retShape = (Shape)DIHelper.getInstance().getLocalProp(shapeString);
		}
		//logger.info("Shape for Type "  type  "[][]"  retShape);
		//shapeHash.put(type, retShape);
		return retShape;
	}
}