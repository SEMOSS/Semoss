package prerna.ui.components.playsheets.datamakers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc.PKQLRunner;

public class PKQLTransformation extends AbstractTransformation {

	private static final Logger LOGGER = LogManager.getLogger(PKQLTransformation.class.getName());
	public static final String METHOD_NAME = "pkql";
	private PKQLRunner runner;

	public static final String EXPRESSION = "EXPRESSION";

	private List<String> parsedPkqls = new Vector<String>();
	private Map<String, Object> feData = new HashMap<String, Object>();
	
	
	IDataMaker dm;

	@Override
	public void setProperties(Map<String, Object> props) {
		//TODO: validate hash and set values
		this.props = props;
	}

	@Override
	public void setDataMakers(IDataMaker... dms){
		this.dm = (IDataMaker) dms[0];
	}

	@Override
	public void setDataMakerComponent(DataMakerComponent dmc){
		LOGGER.info("dmc is not needed for pkql");
	}

	@Override
	public void setTransformationType(Boolean preTransformation){
		LOGGER.info("pre transformation is not needed for pkql");
	}

	@Override
	public void runMethod() {
		// check how long runner response array is
		int numOldCmds = runner.getResults().size();
		
		String expression = props.get(EXPRESSION) + "";		
		runner.runPKQL(expression, (ITableDataFrame) this.dm);
//		this.dm = runner.getDataFrame();
		this.feData.putAll(runner.getFeData());
		
		// running the pkql may have changed the datamaker:::::::::::::::::::::::::::::::::::::::::::::::::::::
		// need to remember to set this back into the insight:::::::::::::::::::::::::::::::::::::::::::::::::
		this.dm = runner.getDataFrame();
		
		// store added responses
		List<Map> allCmds = runner.getResults();
		for(int i = numOldCmds ; i < allCmds.size(); i++){
			String cmd = (String) allCmds.get(i).get("command");
			if(cmd != null){
				parsedPkqls.add(cmd);
			}
			else {
				LOGGER.error("this is weird... my runner response doesn't have a PKQL command stored. Skipping for now in terms of adding to recipe");
			}
		}
	}

	@Override
	public Map<String, Object> getProperties() {
		props.put(TYPE, METHOD_NAME);
		return this.props;
	}

	@Override
	public void undoTransformation() {
		LOGGER.info("unable to undo pkql transformation");
	}

	@Override
	public PKQLTransformation copy() {
		PKQLTransformation joinCopy = new PKQLTransformation();
		joinCopy.setDataMakers(dm);
		joinCopy.setId(id);
		joinCopy.runner = this.runner; // keep this shallow so updates can be gotten
		joinCopy.feData = this.feData; // keep this shallow so updates can be gotten
		joinCopy.parsedPkqls = this.parsedPkqls; // keep this shallow so updates can be gotten

		if(props != null) {
			Gson gson = new GsonBuilder().disableHtmlEscaping().serializeSpecialFloatingPointValues().setPrettyPrinting().create();
			String propCopy = gson.toJson(props);
			Map<String, Object> newProps = gson.fromJson(propCopy, new TypeToken<Map<String, Object>>() {}.getType());
			joinCopy.setProperties(newProps);
		}

		return joinCopy;
	}
	
	public Map<String, Object> getFeData(){
		return this.feData;
	}
	
	public void setRunner(PKQLRunner runner){
		this.runner = runner;
	}
	
	public List<String> getPkql() {
		return parsedPkqls;
	}
}
