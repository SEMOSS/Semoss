package prerna.rdf.query.builder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.query.util.SEMOSSQuery;
import prerna.rdf.query.util.SEMOSSQueryHelper;

public class ScatterPlotQueryBuilder extends AbstractQueryBuilder{
	static final Logger logger = LogManager.getLogger(ScatterPlotQueryBuilder.class.getName());

	String labelColName;
	String xAxisColName;
	String yAxisColName;
	String zAxisColName;
	String xAxisMathFunc;
	String yAxisMathFunc;
	String zAxisMathFunc;
	String seriesColName;
	
	public ScatterPlotQueryBuilder(String labelColName, String xAxisColName, String yAxisColName, String zAxisColName, String xAxisMathFunc, String yAxisMathFunc, String zAxisMathFunc, String seriesColName, ArrayList<Hashtable<String, String>> parameters, SEMOSSQuery baseQuery) {
		super(parameters, baseQuery);
		this.labelColName = labelColName;
		this.xAxisColName = xAxisColName;
		this.yAxisColName = yAxisColName;
		this.zAxisColName = zAxisColName;
		this.xAxisMathFunc = xAxisMathFunc;
		this.yAxisMathFunc = yAxisMathFunc;
		this.zAxisMathFunc = zAxisMathFunc;
		this.seriesColName = seriesColName;
	}
	
    public void buildQuery () {
        ArrayList<String> varNames = uniqifyColNames(Arrays.asList( labelColName, xAxisColName, yAxisColName, zAxisColName, seriesColName ));
        ArrayList<String> groupList = new ArrayList<String>();

        // the order for the return variables is series, label, x, y, z
        // start with series
        if(seriesColName != null){
        	logger.info("Adding series: " + seriesColName);
        	addReturnVariable(seriesColName, varNames.get(4), baseQuery, "false");
            groupList.add(seriesColName);
        }
        
        //label
        logger.info("Adding label: " + labelColName);
        addReturnVariable(labelColName, varNames.get(0), baseQuery, "false");
        groupList.add(labelColName);


        //x
        logger.info("Adding x-axis variable: " + xAxisColName);
        addReturnVariable(xAxisColName, varNames.get(1), baseQuery, xAxisMathFunc);
        if(xAxisMathFunc.equals("false")){
        	groupList.add(xAxisColName);
        }
        
        //y
        logger.info("Adding y-axis variable: " + yAxisColName);
        addReturnVariable(yAxisColName, varNames.get(2), baseQuery, yAxisMathFunc);
        if(yAxisColName.equals("false")){
        	groupList.add(yAxisColName);
        }
        
        //z if it is not null
        if(zAxisColName != null){
        	logger.info("Adding z-axis variable: " + zAxisColName);
        	addReturnVariable(zAxisColName, varNames.get(3), baseQuery, zAxisMathFunc);
        }

        //add them as group by
        if(!groupList.isEmpty()){
        	logger.info("Adding GroupBy to query: " + groupList);
        	SEMOSSQueryHelper.addGroupByToQuery(groupList, baseQuery);
        }
        
		if(!parameters.isEmpty()) {
			logger.info("Adding parameters: " + parameters);
			addParam("Main");
		}
        
        createQuery();
        logger.info("Created ScatterPlot Query: " + query);
    }

}
