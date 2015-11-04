package prerna.ui.components.playsheets.datamakers;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.IMatcher;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.ExactStringMatcher;
import prerna.ds.ExactStringOuterJoinMatcher;
import prerna.ds.ExactStringPartialOuterJoinMatcher;

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

	@Override
	public void setProperties(Map<String, Object> props) {
		//TODO: validate hash and set values
		this.props = props;
	}

	@Override
	public void setDataMakers(IDataMaker... dm){
		this.dm = (ITableDataFrame) dm[0];
		this.nextDm = (ITableDataFrame) dm[1];
	}
	
	@Override
	public void setDataMakerComponent(DataMakerComponent dmc){
		this.dmc = dmc;
	}
	
	@Override
	public void setTransformationType(Boolean preTransformation){
		if(preTransformation){
			LOGGER.error("Cannot run join as pretransformation");
		}
	}

	@Override
	public void runMethod() {
		Method method = null;
		try {
			method = dm.getClass().getMethod(METHOD_NAME, ITableDataFrame.class, String.class, String.class, double.class, IMatcher.class);
			LOGGER.info("Successfully got method : " + METHOD_NAME);
			
			String t1Col = this.props.get(COLUMN_ONE_KEY) +"";
			String t2Col = this.props.get(COLUMN_TWO_KEY) +"";
			Double match = 1.0; // this should be in properties as well
			String joinType = (String) this.props.get(JOIN_TYPE);
			if(joinType == null) {
				joinType = "inner";
			}
			
			IMatcher matcher = null;
			switch(joinType) {
				case "inner" : matcher = new ExactStringMatcher(); 
					break;
				case "partial" : matcher = new ExactStringPartialOuterJoinMatcher(); 
					break;
				case "outer" : matcher = new ExactStringOuterJoinMatcher();
					break;
				default : matcher = new ExactStringMatcher(); 
			}
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
		return;
	}

	@Override
	public Map<String, Object> getProperties() {
		props.put(TYPE, METHOD_NAME);
		return this.props;
	}

	@Override
	public void undoTransformation() {
		String[] allCols = nextDm.getColumnHeaders();
		List<String> addedCols = new ArrayList<String>();
		for(int i = 0; i < allCols.length; i++) {
			String val = allCols[i];
			if(val.equals(props.get(COLUMN_TWO_KEY) + "")) {
				continue;
			}
			addedCols.add(val);
		}
		Method method = null;
		try {
			method = dm.getClass().getMethod(UNDO_METHOD_NAME, String.class);
			LOGGER.info("Successfully got method : " + METHOD_NAME);
			
			// iterate from root to top for efficiency in removing connections
			for(int i = addedCols.size()-1; i >= 0; i--) {
				method.invoke(dm, addedCols.get(i));
				LOGGER.info("Successfully invoked method : " + METHOD_NAME);
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
	
}
