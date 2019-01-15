package prerna.ui.components.playsheets.datamakers;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.QueryStruct;
import prerna.ds.TinkerFrame;
import prerna.ds.h2.H2Frame;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.GraphDataModel;
import prerna.query.querystruct.SelectQueryStruct;

public class JoinTransformation extends AbstractTransformation {

	private static final Logger LOGGER = LogManager.getLogger(JoinTransformation.class.getName());
	public static final String METHOD_NAME = "join";
	public static final String UNDO_METHOD_NAME = "removeColumn";
	public static final String COLUMN_ONE_KEY = "table1Col";
	public static final String COLUMN_TWO_KEY = "table2Col";
	public static final String JOIN_TYPE = "joinType";
	public static final String INNER = "inner";
	public static final String PARTIAL = "partial";
	public static final String OUTER = "outer";

	
	DataMakerComponent dmc;
	IDataMaker dm;
	IDataMaker nextDm;
	List<String> prevHeaders = null;
	List<String> addedColumns = new ArrayList<String>();

	boolean preTransformation = false;

	@Override
	public void setProperties(Map<String, Object> props) {
		//TODO: validate hash and set values
		this.props = props;
	}

	@Override
	public void setDataMakers(IDataMaker... dms){
		if(preTransformation) {
			this.dm = (IDataMaker) dms[0];
		} else {
			this.dm = (IDataMaker) dms[0];
			if(dms.length>1){
				this.nextDm = (IDataMaker) dms[1];
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
		// if its a graph data model, there is no joining necessary
		// the process will always be that we continue to add to the gdm
		if (dm instanceof GraphDataModel) {
			((GraphDataModel) dm).setOverlay(true);
			return;
		}

		// the run method will either append to the component to limit the
		// construction of the new component
		// otherwise, it will perform the actual joining between two components
		if (!preTransformation) {
			// need to get the added columns
			processPostTransformation();
		} else {
			this.prevHeaders = Arrays.asList(((ITableDataFrame) dm).getColumnHeaders());
			// if the table is currently empty, there is nothing to optimize on
			if (this.prevHeaders.isEmpty()) {
				return;
			}
			QueryStruct qs = dmc.getQueryStruct();

			// if stringmap already contains the filters, then it is a hard filter
			// otherwise, add based on what is currently in the tree
			if (qs != null && !qs.hasFiltered(props.get(COLUMN_TWO_KEY) + "")) {
				SelectQueryStruct qs2 = new SelectQueryStruct();
				Iterator<IHeadersDataRow> rowIt = null;
				if (dm instanceof H2Frame) {
					qs2.addSelector(((ITableDataFrame) dm).getName(), props.get(COLUMN_TWO_KEY).toString());
					rowIt = ((ITableDataFrame) dm).query(qs2);
				} else {
					// tinker
					qs2.addSelector(props.get(COLUMN_TWO_KEY).toString(), QueryStruct.PRIM_KEY_PLACEHOLDER);
					rowIt = ((ITableDataFrame) dm).query(qs2);
				}

				List<Object> uris = new Vector<Object>();
				while (rowIt.hasNext()) {
					uris.add("\"" + rowIt.next() + "\"");
				}

				String columnTwo = props.get(COLUMN_TWO_KEY).toString();
				qs.addFilter(columnTwo, "=", uris);
			}

			// add the join as a post transformation
			dmc.getPostTrans().add(0, this);
			preTransformation = false;
		}
  }

	
	private void processPostTransformation(){
		String[] allCols = ((ITableDataFrame) dm).getColumnHeaders();
		for(int i = 0; i < allCols.length; i++) {
			String val = allCols[i];
			if(!this.prevHeaders.contains(val)) {
				addedColumns.add(val);
			}
		}
		if(this.props.get(JOIN_TYPE) != null && ((String) this.props.get(JOIN_TYPE)).equals("partial")){
			String colName = this.props.get(COLUMN_ONE_KEY) +"";
			if(dm instanceof TinkerFrame){
				((TinkerFrame)dm).insertBlanks(colName, addedColumns);
			}
		}
//		((TinkerFrame)dm).removeExtraneousNodes(); though this call makes sense in terms of keeping the tinker free of unnecessary nodes, it is quite slow. Going to try to call this only when necessary (serializing)
	}
	
	@Override
	public Map<String, Object> getProperties() {
		props.put(TYPE, METHOD_NAME);
		return this.props;
	}

	@Override
	public void undoTransformation() {
		if(dm instanceof GraphDataModel){
			((GraphDataModel) dm).undoData();
			return;
		}
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

	public List<String> getAddedColumns() {
		return addedColumns;
	}
}
