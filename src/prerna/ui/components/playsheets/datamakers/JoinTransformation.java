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
import com.google.gson.reflect.TypeToken;

import prerna.algorithm.api.IMatcher;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.BTreeDataFrame;
import prerna.ds.InstanceMatcher;
import prerna.ds.InstanceOuterJoinMatcher;
import prerna.ds.InstancePartialOuterJoinMatcher;
import prerna.ds.TinkerFrame;
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
	List<String> prevHeaders = null;
	List<String> addedColumns = new ArrayList<String>();

	IMatcher matcher;

	boolean preTransformation = false;

	@Override
	public void setProperties(Map<String, Object> props) {
		//TODO: validate hash and set values
		this.props = props;
	}

	@Override
	public void setDataMakers(IDataMaker... dms){
		if(preTransformation) {
			this.dm = (ITableDataFrame) dms[0];
			this.prevHeaders = Arrays.asList(dm.getColumnHeaders());
		} else {
			this.dm = (ITableDataFrame) dms[0];
			if(dms.length>1){
				this.nextDm = (ITableDataFrame) dms[1];
			}
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
		if(dm instanceof BTreeDataFrame) {
			getMatcher();
		}
		
		//Store the new columns that will be added to dm
		if(nextDm != null) { // this will only be the case for BTREE
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
			// We are no longer using this as the new data is added directly to the main frame
			if(dm instanceof BTreeDataFrame) {
				Method method = null;
				try {
					method = dm.getClass().getMethod(METHOD_NAME, ITableDataFrame.class, String.class, String.class, double.class, IMatcher.class);
					LOGGER.info("Successfully got method : " + METHOD_NAME);

					String t1Col = this.props.get(COLUMN_ONE_KEY) +"";
					String t2Col = this.props.get(COLUMN_TWO_KEY) +"";
					Double match = 1.0; // this should be in properties as well

					method.invoke(dm, this.nextDm, t1Col, t2Col, match, matcher);
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
			}
			else { // instance of Tinker
				// need to get the added columns
				String[] allCols = dm.getColumnHeaders();
				for(int i = 0; i < allCols.length; i++) {
					String val = allCols[i];
					if(!this.prevHeaders.contains(val)) {
						addedColumns.add(val);
					}
				}
//				((TinkerFrame)dm).removeExtraneousNodes(); though this call makes sense in terms of keeping the tinker free of unnecessary nodes, it is quite slow. Going to try to call this only when necessary (serializing)
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
			if(!stringMap.containsKey(props.get(COLUMN_TWO_KEY) + "")) {
				if(dm instanceof BTreeDataFrame) {
					//but actually, also need to consider the type of matcher
					if(this.matcher.getQueryModType().equals(IMatcher.MATCHER_ACTION.BIND)) {
						List<Object> queryModList = this.matcher.getQueryModList(dm, props.get(COLUMN_ONE_KEY) + "", dmc.getEngine(), props.get(COLUMN_TWO_KEY) + "");
						stringMap.put(props.get(COLUMN_TWO_KEY) + "", queryModList );
						builderData.setFilterData(stringMap);
					} else {
						LOGGER.error("Matcher type " + this.matcher.getQueryModType() + " is not supported in join method.");
					}
				} else {
					stringMap.put(props.get(COLUMN_TWO_KEY) + "", Arrays.asList(dm.getUniqueRawValues(props.get(COLUMN_ONE_KEY) + "")) );
					builderData.setFilterData(stringMap);
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
		//need this in copy for btree join method
		joinCopy.matcher = this.matcher;

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
			case "inner" : this.matcher = new InstanceMatcher(); 
			break;
			case "partial" : this.matcher = new InstancePartialOuterJoinMatcher(); 
			break;
			case "outer" : this.matcher = new InstanceOuterJoinMatcher();
			break;
			default : this.matcher = new InstanceMatcher(); 
			}
		}
	}

}
