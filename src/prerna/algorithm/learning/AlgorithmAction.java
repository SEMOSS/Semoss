package prerna.algorithm.learning;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import prerna.algorithm.api.IAnalyticActionRoutine;
import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.moa.HoeffdingTreeAlgorithm;
import prerna.algorithm.learning.supervized.MatrixRegressionAlgorithm;
import prerna.algorithm.learning.supervized.NumericalCorrelationAlgorithm;
import prerna.algorithm.learning.weka.WekaAprioriAlgorithm;
import prerna.algorithm.learning.weka.WekaClassification;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.ui.components.playsheets.datamakers.ISEMOSSAction;

public class AlgorithmAction implements ISEMOSSAction {

	private static final Logger LOGGER = LogManager.getLogger(AlgorithmAction.class.getName());
	private static final String ACTION_NAME = "algorithmAction";				// the name to distinguish the action as an algorithm action in insight makeup
	private static final String METHOD_NAME = "performAnalyticAction";			// the name of the method in all data makers to perform an algorithm transformation
	public static final String ALGORITHM_TYPE = "algorithmType";				// key in the properties to determine the type of algorithm being run

	// the list of default algorithm actions
	public static final String MATRIX_REGRESSION = "matrix_regression";
	public static final String NUMERICAL_CORRELATION = "numerical_correlation";
	public static final String ASSOCATION_LEARNING = "association_learning";
	public static final String J48_CLASSIFICATION = "j48";
	public static final String HOEFFDING_TREE_CLASSIFICATION = "hoeffding_tree";

	private Map<String, Object> props;
	private DataMakerComponent dmc;
	private ITableDataFrame dm;
	private String id;

	@Override
	//TODO: need to figure out how the routines themselves will be obtained to override defaults in rdf map and engine prop
	public Object runMethod() {
		Method method = null;
		IAnalyticActionRoutine routine = null;
		try {
			method = dm.getClass().getMethod(METHOD_NAME, IAnalyticActionRoutine.class);
			LOGGER.info("Successfully got method : " + METHOD_NAME);
			String type = (String) this.props.get(ALGORITHM_TYPE);
			if(type == null) {
				throw new IllegalArgumentException("Algorithm type not specified for Algorithm Transformation");
			}

			switch(type) {
			case MATRIX_REGRESSION : routine = new MatrixRegressionAlgorithm(); 
				break;
			case NUMERICAL_CORRELATION : routine = new NumericalCorrelationAlgorithm(); 
				break;
			case ASSOCATION_LEARNING : routine = new WekaAprioriAlgorithm(); 
				break;
			case J48_CLASSIFICATION : routine = new WekaClassification(); 
				break;
			case HOEFFDING_TREE_CLASSIFICATION : routine = new HoeffdingTreeAlgorithm(); 
				break;
			}
			if(routine == null) {
				throw new IllegalArgumentException("Algorithm routine " + type + " cannot be found.");
			}

			routine.setSelectedOptions(props);
			method.invoke(dm, routine);
			LOGGER.info("Successfully invoked method : " + METHOD_NAME);

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
		
		return routine.getAlgorithmOutput();
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
	public void setProperties(Map<String, Object> props) {
		this.props = props;
	}

	@Override
	public Map<String, Object> getProperties() {
		props.put(TYPE, ACTION_NAME);
		return this.props;
	}
	
	@Override
	public void setId(String id) {
		this.id = id;
	}
	
	@Override
	public String getId() {
		return this.id;
	}

	@Override
	public AlgorithmAction copy() {
		
		AlgorithmAction copy = new AlgorithmAction();
		copy.setDataMakerComponent(dmc);
		copy.setDataMakers(dm);
		copy.setId(id);
		
		if(props != null) {
			Gson gson = new GsonBuilder().disableHtmlEscaping().serializeSpecialFloatingPointValues().setPrettyPrinting().create();
			String propCopy = gson.toJson(props);
			Map<String, Object> newProps = gson.fromJson(propCopy, new TypeToken<Map<String, Object>>() {}.getType());
			copy.setProperties(newProps);
		}
		
		return copy;
	}
}
