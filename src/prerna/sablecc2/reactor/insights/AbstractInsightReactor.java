package prerna.sablecc2.reactor.insights;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.reactor.AbstractReactor;

public abstract class AbstractInsightReactor extends AbstractReactor {
	
	// used for running insights
	protected static final String ENGINE_KEY = "engine";
	protected static final String RDBMS_ID = "id";
	protected static final String PARAM_KEY = "params";

	// used for saving a base insight
	protected static final String INSIGHT_NAME = "insightName";
	protected static final String RECIPE = "recipe";
	protected static final String LAYOUT = "layout";
	protected static final String IMAGE_URL = "image";

	protected String getEngine() {
		// look at all the ways the insight panel could be passed
		// look at store if it was passed in
		GenRowStruct genericEngineGrs = this.store.getNoun(ENGINE_KEY);
		if(genericEngineGrs != null && !genericEngineGrs.isEmpty()) {
			return (String) genericEngineGrs.get(0);
		}
		
		// see if it is in the curRow
		// if it was passed directly in as a variable
		List<NounMetadata> stringNouns = this.curRow.getNounsOfType(PixelDataType.CONST_STRING);
		if(stringNouns != null && !stringNouns.isEmpty()) {
			return (String) stringNouns.get(0).getValue();
		}
		
		// well, you are out of luck
		return null;
	}
	
	/**
	 * This can either be passed specifically using the insightName key
	 * Or it is the second input in a list of values
	 * Save(engineName, insightName)
	 * @return
	 */
	protected String getInsightName() {
		// look at all the ways the insight panel could be passed
		// look at store if it was passed in
		GenRowStruct genericEngineGrs = this.store.getNoun(INSIGHT_NAME);
		if(genericEngineGrs != null && !genericEngineGrs.isEmpty()) {
			return (String) genericEngineGrs.get(0);
		}
		
		// see if it is in the curRow
		// if it was passed directly in as a variable
		// this will be the second input! (i.e. index 1)
		List<NounMetadata> stringNouns = this.curRow.getNounsOfType(PixelDataType.CONST_STRING);
		if(stringNouns != null && !stringNouns.isEmpty()) {
			return (String) stringNouns.get(1).getValue();
		}
		
		// well, you are out of luck
		return null;
	}
	
	protected int getRdbmsId() {
		// see if it was passed directly in with the lower case key ornaments
		GenRowStruct genericIdGrs = this.store.getNoun(RDBMS_ID);
		if(genericIdGrs != null && !genericIdGrs.isEmpty()) {
			return (int) genericIdGrs.get(0);
		}
		
		// see if it is in the curRow
		// if it was passed directly in as a variable
		List<NounMetadata> intNouns = this.curRow.getNounsOfType(PixelDataType.CONST_INT);
		if(intNouns != null && !intNouns.isEmpty()) {
			return (int) intNouns.get(0).getValue();
		}
		
		// well, you are out of luck
		return -1;
	}
	
	protected String[] getRecipe() {
		// it must be passed directly into its own grs
		GenRowStruct genericRecipeGrs = this.store.getNoun(RECIPE);
		if(genericRecipeGrs != null && !genericRecipeGrs.isEmpty()) {
			int size = genericRecipeGrs.size();
			String[] recipe = new String[size];
			for(int i = 0; i < size; i++) {
				recipe[i] = genericRecipeGrs.get(i).toString();
			}
			return recipe;
		}
		
		// well, you are out of luck
		return new String[0];
	}
	
	protected String getLayout() {
		// it must be passed directly into its own grs
		GenRowStruct genericLayoutGrs = this.store.getNoun(LAYOUT);
		if(genericLayoutGrs != null && !genericLayoutGrs.isEmpty()) {
			return genericLayoutGrs.get(0).toString();
		}
		
		// well, you are out of luck
		return null;
	}
	
	protected String getImageURL() {
		GenRowStruct genericBaseURLGrs = this.store.getNoun(IMAGE_URL);
		if(genericBaseURLGrs != null && !genericBaseURLGrs.isEmpty()) {
			try {
				String imageURL = URLDecoder.decode(genericBaseURLGrs.get(0).toString(), "UTF-8");
				return imageURL;
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		// well, you are out of luck
		return null;
	}
	
	protected Object getParams() {
		GenRowStruct paramGrs = this.store.getNoun(PARAM_KEY);
		if(paramGrs == null || paramGrs.isEmpty()) {
			return null;
		}
		
		if(paramGrs.size() == 1) {
			return paramGrs.get(0);
		} else {
			List<Object> params = new ArrayList<Object>();
			for(int i = 0; i < paramGrs.size(); i++) {
				params.add(paramGrs.get(i));
			}
			return params;
		}
	}
}
