package prerna.ui.components.playsheets.datamakers;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import prerna.algorithm.api.ITableDataFrame;
import prerna.util.Constants;

public class BinningTransformation extends AbstractTransformation {

	private static final Logger LOGGER = LogManager.getLogger(AbstractTransformation.class.getName());
	public static final String METHOD_NAME = "binNumericColumn";
	public static final String UNDO_METHOD_NAME = "removeColumn";
	public static final String COLUMN_TO_BIN = "colName";

	DataMakerComponent dmc;
	ITableDataFrame dm;
	
	@Override
	public Map<String, Object> getProperties() {
		props.put(TYPE, METHOD_NAME);
		return this.props;
	}

	@Override
	public void setDataMakers(IDataMaker... dms) {
		this.dm = (ITableDataFrame) dms[0];
	}

	@Override
	public void runMethod() {
		Method method = null;
		try {
			method = dm.getClass().getMethod(METHOD_NAME, String.class);
			LOGGER.info("Successfully got method : " + METHOD_NAME);
			
			method.invoke(dm, props.get(COLUMN_TO_BIN));
			LOGGER.info("Successfully invoked method : " + METHOD_NAME);
		} catch (NoSuchMethodException | SecurityException e) {
			LOGGER.error(Constants.STACKTRACE, e);
		} catch (IllegalAccessException e) {
			LOGGER.error(Constants.STACKTRACE, e);
		} catch (IllegalArgumentException e) {
			LOGGER.error(Constants.STACKTRACE, e);
		} catch (InvocationTargetException e) {
			LOGGER.error(Constants.STACKTRACE, e);
		}
		return;
	}

	@Override
	public void setDataMakerComponent(DataMakerComponent dmc) {
		this.dmc = dmc;
	}

	@Override
	public void setTransformationType(Boolean preTransformation) {
		if(preTransformation){
			LOGGER.error("Cannot run math as pre-transformation");
		}
	}

	@Override
	public void undoTransformation() {
		String[] allCols = dm.getColumnHeaders();
		List<String> addedCols = new ArrayList<String>();
		for(int i = 0; i < allCols.length; i++) {
			String val = allCols[i];
			if(val.equals(props.get(COLUMN_TO_BIN) + "")) {
				continue;
			}
			addedCols.add(val);
		}
		Method method = null;
		try {
			method = dm.getClass().getMethod(UNDO_METHOD_NAME, String.class);
			LOGGER.info("Successfully got method : " + UNDO_METHOD_NAME);
			
			// iterate from root to top for efficiency in removing connections
			for(int i = addedCols.size()-1; i >= 0; i--) {
				method.invoke(dm, addedCols.get(i));
				LOGGER.info("Successfully invoked method : " + UNDO_METHOD_NAME);
			}
		} catch (NoSuchMethodException | SecurityException e) {
			LOGGER.error(Constants.STACKTRACE, e);
		} catch (IllegalAccessException e) {
			LOGGER.error(Constants.STACKTRACE, e);
		} catch (IllegalArgumentException e) {
			LOGGER.error(Constants.STACKTRACE, e);
		} catch (InvocationTargetException e) {
			LOGGER.error(Constants.STACKTRACE, e);
		}
	}

	@Override
	public BinningTransformation copy() {

		BinningTransformation copy = new BinningTransformation();
		
		copy.setDataMakerComponent(this.dmc);
		copy.setDataMakers(this.dm);
		copy.setId(this.id);

		if(this.props != null) {
			Gson gson = new GsonBuilder().disableHtmlEscaping().serializeSpecialFloatingPointValues().setPrettyPrinting().create();
			String propCopy = gson.toJson(this.props);
			Map<String, Object> newProps = gson.fromJson(propCopy, new TypeToken<Map<String, Object>>() {}.getType());
			copy.setProperties(newProps);
		}
		
		return copy;
	}

}
