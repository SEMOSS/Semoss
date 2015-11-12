package prerna.algorithm.learning;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.IAnalyticTransformationRoutine;
import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.similarity.DatasetSimilarity;
import prerna.algorithm.learning.unsupervised.clustering.ClusteringRoutine;
import prerna.algorithm.learning.unsupervised.clustering.MultiClusteringRoutine;
import prerna.algorithm.learning.unsupervised.outliers.FastOutlierDetection;
import prerna.algorithm.learning.unsupervised.outliers.LOF;
import prerna.algorithm.learning.unsupervised.som.SOMRoutine;
import prerna.ui.components.playsheets.datamakers.AbstractTransformation;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.IDataMaker;

public class AlgorithmTransformation extends AbstractTransformation {

	private static final Logger LOGGER = LogManager.getLogger(AlgorithmTransformation.class.getName());
	private static final String TRANSFORMATION_NAME = "algorithmTransformation";
	private static final String METHOD_NAME = "performAnalyticTransformation";
	public static final String UNDO_METHOD_NAME = "removeColumn";
	public static final String ALGORITHM_TYPE = "algorithmType";

	public static final String CLUSTERING = "clustering";
	public static final String MULTI_CLUSTERING = "multi_clustering";
	public static final String LOCAL_OUTLIER_FACTOR = "lof";
	public static final String FAST_OUTLIERS = "fast_outlier";
	public static final String SELF_ORGANIZING_MAP = "som";
	public static final String SIMILARITY = "similarity";

	List<String> addedColumns;
	DataMakerComponent dmc;
	ITableDataFrame dm;

	@Override
	//TODO: need to figure out how the routines themselves will be obtained to override defaults in rdf map and engine prop
	public void runMethod() {
		Method method = null;
		try {
			method = dm.getClass().getMethod(METHOD_NAME, IAnalyticTransformationRoutine.class);
			LOGGER.info("Successfully got method : " + METHOD_NAME);
			String type = (String) this.props.get(ALGORITHM_TYPE);
			if(type == null) {
				throw new IllegalArgumentException("Algorithm type not specified for Algorithm Transformation");
			}

			IAnalyticTransformationRoutine routine = null;
			switch(type) {
			case CLUSTERING : routine = new ClusteringRoutine(); 
				break;
			case MULTI_CLUSTERING : routine = new MultiClusteringRoutine(); 
				break;
			case LOCAL_OUTLIER_FACTOR : routine = new LOF(); 
				break;
			case FAST_OUTLIERS : routine = new FastOutlierDetection(); 
				break;
			case SELF_ORGANIZING_MAP : routine = new SOMRoutine(); 
				break;
			case SIMILARITY : routine = new DatasetSimilarity(); 
				break;
			}
			if(routine == null) {
				throw new IllegalArgumentException("Algorithm routine " + type + " cannot be found.");
			}
			
			routine.setSelectedOptions(props);
			method.invoke(dm, routine);
			LOGGER.info("Successfully invoked method : " + METHOD_NAME);
			this.addedColumns = routine.getChangedColumns();
			
		} catch (NoSuchMethodException | SecurityException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e.getMessage());
		} catch (InvocationTargetException e) {
			e.printStackTrace();
			String message = "";
			if(e.getMessage()!= null && e.getMessage().isEmpty()) {
				message = e.getMessage();
			} else {
				message = e.getTargetException().getMessage();
			}
			throw new IllegalArgumentException(message);
		}
		return;

	}

	@Override
	public void setDataMakers(IDataMaker... dms) {
		this.dm = (ITableDataFrame) dms[0];
	}

	@Override
	public void setDataMakerComponent(DataMakerComponent dmc) {
		this.dmc = dmc;
	}

	@Override
	public void setTransformationType(Boolean preTransformation) {
		if(preTransformation){
			LOGGER.error("Cannot run performAction as pretransformation");
		}
	}

	@Override
	public void setProperties(Map<String, Object> props) {
		//TODO: validate hash and set values
		this.props = props;
	}

	@Override
	public Map<String, Object> getProperties() {
		props.put(TYPE, TRANSFORMATION_NAME);
		return this.props;
	}

	public List<String> getAddedColumns() {
		return this.addedColumns;
	}

	@Override
	public void undoTransformation() {
		Method method = null;
		try {
			method = dm.getClass().getMethod(UNDO_METHOD_NAME, String.class);
			LOGGER.info("Successfully got method : " + METHOD_NAME);
			
			// iterate from root to top for efficiency in removing connections
			for(int i = addedColumns.size()-1; i >= 0; i--) {
				method.invoke(dm, addedColumns.get(i));
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
