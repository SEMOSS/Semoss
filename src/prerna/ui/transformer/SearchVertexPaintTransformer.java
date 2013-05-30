package prerna.ui.transformer;

import java.awt.Paint;
import java.util.Hashtable;

import org.apache.commons.collections15.Transformer;
import org.apache.log4j.Logger;

import prerna.om.DBCMVertex;
import prerna.ui.helpers.TypeColorShapeTable;
import prerna.util.Constants;

public class SearchVertexPaintTransformer implements Transformer <DBCMVertex, Paint> {
	
	Hashtable <String, String> verticeURI2Show = null;
	Logger logger = Logger.getLogger(getClass());
	
	public SearchVertexPaintTransformer(Hashtable verticeURI2Show)
	{
		this.verticeURI2Show = verticeURI2Show;
	}
	
	public void setVertHash(Hashtable verticeURI2Show)
	{
		this.verticeURI2Show = verticeURI2Show;
	}
	public Hashtable getVertHash()
	{
		return verticeURI2Show;
	}
	
	@Override
	public Paint transform(DBCMVertex arg0) {
		// get the DI Helper to find what is the property we need to get for vertex
		// based on that get that property and return it

		Paint type = TypeColorShapeTable.getInstance().getColor(Constants.TRANSPARENT);

		if(verticeURI2Show != null)
		{
			String URI = (String)arg0.getProperty(Constants.URI);
			logger.debug("URI " + URI);
			if(verticeURI2Show.containsKey(URI))
			{
				String propType = (String)arg0.getProperty(Constants.VERTEX_TYPE);
				String vertName = (String)arg0.getProperty(Constants.VERTEX_NAME);
				logger.debug("Found the URI");
				type = TypeColorShapeTable.getInstance().getColor(propType, vertName);
			}
		}
		return type;
	}
}
