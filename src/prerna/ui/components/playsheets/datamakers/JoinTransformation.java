package prerna.ui.components.playsheets.datamakers;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.internal.StringMap;
import com.google.gson.reflect.TypeToken;

import prerna.algorithm.api.IMatcher;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.ExactStringMatcher;
import prerna.ds.ExactStringOuterJoinMatcher;
import prerna.ds.ExactStringPartialOuterJoinMatcher;
import prerna.rdf.query.builder.AbstractQueryBuilder;
import prerna.rdf.query.builder.QueryBuilderData;

public class JoinTransformation extends AbstractTransformation {

	private static final Logger LOGGER = LogManager.getLogger(JoinTransformation.class.getName());
	public static final String METHOD_NAME = "join";
	public static final String UNDO_METHOD_NAME = "removeColumn";
	public static final String COLUMN_ONE_KEY = "table1Col";
	public static final String COLUMN_TWO_KEY = "table2Col";
	public static final String JOIN_TYPE = "joinType";

	DataMakerComponent dmc;
	ITableDataFrame dm;
	ITableDataFrame nextDm;
	List<String> addedColumns = new ArrayList<>();

	IMatcher matcher;
	
	boolean preTransformation = false;
	
	@Override
	public void setProperties(Map<String, Object> props) {
		//TODO: validate hash and set values
		this.props = props;
	}

	@Override
	public void setDataMakers(IDataMaker... dm){
		if(preTransformation) {
			this.dm = (ITableDataFrame) dm[0];
		} else {
			this.dm = (ITableDataFrame) dm[0];
			this.nextDm = (ITableDataFrame) dm[1];
		}
	}
	
	@Override
	public void setDataMakerComponent(DataMakerComponent dmc){
		this.dmc = dmc;
	}
	
	@Override
	public void setTransformationType(Boolean preTransformation){
		this.preTransformation = preTransformation;
	}

	@Override
	public void runMethod() {
		getMatcher();
		
		//Store the new columns that will be added to dm
		if(nextDm != null) {
			String[] allCols = nextDm.getColumnHeaders();
			for(int i = 0; i < allCols.length; i++) {
				String val = allCols[i];
				if(val.equals(props.get(COLUMN_TWO_KEY) + "")) {
					continue;
				}
				addedColumns.add(val);
			}
		}
		
		//the run method will either append to the component to limit the construction of the new component
		//otherwise, it will perform the actual joining between two components
		if(!preTransformation) {
			Method method = null;
			try {
				method = dm.getClass().getMethod(METHOD_NAME, ITableDataFrame.class, String.class, String.class, double.class, IMatcher.class);
				LOGGER.info("Successfully got method : " + METHOD_NAME);
				
				String t1Col = this.props.get(COLUMN_ONE_KEY) +"";
				String t2Col = this.props.get(COLUMN_TWO_KEY) +"";
				Double match = 1.0; // this should be in properties as well
				
				if(this.matcher == null) {
					
				}
				
				method.invoke(dm, this.nextDm, t1Col, t2Col, match, this.matcher);
				LOGGER.info("Successfully invoked method : " + METHOD_NAME);
	
			} catch (NoSuchMethodException | SecurityException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				e.printStackTrace();
			}
		} else {
			QueryBuilderData builderData = dmc.getBuilderData();
			Map<String, List<Object>> stringMap;
			if(builderData.getFilterData() != null && !builderData.getFilterData().isEmpty()) {
	               stringMap = builderData.getFilterData();
	        } else {
	               stringMap = new HashMap<String, List<Object>>();
	        }
	        //if stringmap already contains the filters, then it is a hard filter
	        //otherwise, add based on what is currently in the tree
	        if(!stringMap.containsKey(props.get(COLUMN_ONE_KEY) + "")) {
	        	//but actually, also need to consider the type of matcher
	        	if(this.matcher.getType().equals(IMatcher.MATCHER_ACTION.BIND)) {
		        	stringMap.put(props.get(COLUMN_ONE_KEY) + "", Arrays.asList(dm.getUniqueRawValues(props.get(COLUMN_ONE_KEY) + "")) );
			        builderData.setFilterData(stringMap);
	        	} else {
	        		LOGGER.error("Matcher type " + this.matcher.getType() + " is not supported in join method.");
	        	}
	        }
	        
			// add the join as a post transformation
			dmc.getPostTrans().add(0, this);
			preTransformation = false;
		}
	}

	@Override
	public Map<String, Object> getProperties() {
		props.put(TYPE, METHOD_NAME);
		return this.props;
	}

	@Override
	public void undoTransformation() {
		/*String[] allCols = nextDm.getColumnHeaders();
		List<String> addedCols = new ArrayList<String>();
		for(int i = 0; i < allCols.length; i++) {
			String val = allCols[i];
			if(val.equals(props.get(COLUMN_TWO_KEY) + "")) {
				continue;
			}
			addedCols.add(val);
		}*/
		Method method = null;
		try {
			method = dm.getClass().getMethod(UNDO_METHOD_NAME, String.class);
			LOGGER.info("Successfully got method : " + UNDO_METHOD_NAME);
			
			// iterate from leaf to root for efficiency in removing connections
			for(int i = addedColumns.size()-1; i >= 0; i--) {
				method.invoke(dm, addedColumns.get(i));
				LOGGER.info("Successfully invoked method : " + UNDO_METHOD_NAME);
			}
		} catch (NoSuchMethodException | SecurityException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public JoinTransformation copy() {
		JoinTransformation joinCopy = new JoinTransformation();
		joinCopy.setDataMakerComponent(dmc);
		joinCopy.setDataMakers(dm, nextDm);
		joinCopy.setId(id);
		joinCopy.setTransformationType(preTransformation);
		joinCopy.addedColumns = this.addedColumns;
		
		if(props != null) {
			Gson gson = new GsonBuilder().disableHtmlEscaping().serializeSpecialFloatingPointValues().setPrettyPrinting().create();
			String propCopy = gson.toJson(props);
			Map<String, Object> newProps = gson.fromJson(propCopy, new TypeToken<Map<String, Object>>() {}.getType());
			joinCopy.setProperties(newProps);
		}

		return joinCopy;
	}
	
	private void getMatcher() {
		if(this.matcher == null) {
			String joinType = (String) this.props.get(JOIN_TYPE);
			if(joinType == null) {
				joinType = "inner";
			}
			switch(joinType) {
				case "inner" : this.matcher = new ExactStringMatcher(); 
					break;
				case "partial" : this.matcher = new ExactStringPartialOuterJoinMatcher(); 
					break;
				case "outer" : this.matcher = new ExactStringOuterJoinMatcher();
					break;
				default : this.matcher = new ExactStringMatcher(); 
			}
		}
	}
	
}
