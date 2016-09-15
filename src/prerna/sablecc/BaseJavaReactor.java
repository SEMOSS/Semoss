package prerna.sablecc;

import java.sql.Connection;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.H2.H2Frame;

import java.sql.*;

import prerna.engine.api.IEngine;

import java.util.*;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import javax.script.ScriptContext;
import javax.script.ScriptException;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Io.Builder;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import prerna.algorithm.api.IMatcher;
import prerna.algorithm.api.IMetaData;
import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IConstructStatement;
import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.om.TinkerGraphDataModel;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.rdf.query.builder.GremlinBuilder;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLEnum.PKQLReactor;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;
import prerna.ui.components.playsheets.datamakers.JoinTransformation;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Console;
import prerna.util.Constants;
import prerna.util.MyGraphIoRegistry;
import prerna.util.Utility;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.rosuda.REngine.Rserve.RserveException;

import prerna.algorithm.api.IMatcher;
import prerna.algorithm.api.IMetaData;
import prerna.algorithm.api.IMetaData.DATA_TYPES;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.AbstractTableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.ds.TinkerMetaData;
import prerna.ds.TinkerMetaHelper;
import prerna.ds.H2.H2Builder.Join;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.r.RRunner;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLEnum.PKQLReactor;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;
import prerna.ui.components.playsheets.datamakers.JoinTransformation;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Constants;
import prerna.util.Utility;


public abstract class BaseJavaReactor extends AbstractReactor{

	ITableDataFrame dataframe = null;
	PKQLRunner pkql = new PKQLRunner();
	boolean frameChanged = false;
	SecurityManager curManager = null;
	SecurityManager reactorManager = null;
	
	public Console System = new Console();
	
	
	public BaseJavaReactor()
	{
		// empty constructor creates a frame
		dataframe = new H2Frame();
	}
	
	public void setCurSecurityManager(SecurityManager curManager)
	{
		this.curManager = curManager;
	}
	
	public void setReactorManager(SecurityManager reactorManager)
	{
		this.reactorManager = reactorManager;
	}
	
	
	public Connection getConnection()
	{
		return ((H2Frame)dataframe).getBuilder().getConnection();
	}
	
	public void setConsole()
	{
		this.System = new Console();
	}

	public BaseJavaReactor(ITableDataFrame frame)
	{
		// empty constructor creates a frame
		dataframe = frame;
	}
	
	// set a variable
	// use this variable if it is needed on the next call
	public void storeVariable(String varName, Object value)
	{
		pkql.setVariableValue(varName, value);
	}
	

	// get the variable back that was set
	public Object getVariable(String varName)
	{
		return pkql.getVariableValue(varName);
	}
	
	// refresh the front end
	// use this call to indicate that you have manipulated the frame or have worked in terms of creating a newer frame
	public void refresh()
	{
		frameChanged = true;
		dataframe.updateDataId();
	}

	public void setPKQLRunner(PKQLRunner pkql)
	{
		this.pkql = pkql;
	}
	// couple of things I need here
	// access to the engines
	public IEngine getEngine(String engineName)
	{
		return null;
	}
	
	// sets the data frame
	public void setDataFrame(ITableDataFrame dataFrame)
	{
		this.dataframe = dataFrame;
	}
	
	public void runPKQL(String pkqlString)
	{
		// this will run PKQL
		System.out.println("Running pkql.. " + pkqlString);
		pkql.runPKQL(pkqlString, dataframe);
		
		myStore.put("RESPONSE", System.out.output);
	}
		
	public void preProcess()
	{
		
	}
	
	
	public void postProcess()
	{
		
	}

	public void filterNode(String columnHeader, String [] instances)
	{
		List <Object> values = new Vector();
		for(int instanceIndex = 0;instanceIndex < instances.length;values.add(instances[instanceIndex]), instanceIndex++);
		dataframe.filter(columnHeader, values);
	}

	public void filterNode(String columnHeader, String value)
	{
		List <Object> values = new Vector();
		values.add(value);
		dataframe.filter(columnHeader, values);
	}

	public void filterNode(String columnHeader, List <Object> instances)
	{
		dataframe.filter(columnHeader, instances);		
	}

	public void runGremlin()
	{
		
	}
	
	@Override
	public String[] getParams() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void set(String key, Object value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void addReplacer(String pattern, Object value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String[] getValues2Sync(String childName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void removeReplacer(String pattern) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object getValue(String key) {
		// TODO Auto-generated method stub
		// return the key for now
		return myStore.get(key);
	}

	@Override
	public void put(String key, Object value) {
		// TODO Auto-generated method stub
		myStore.put(key, value);
		
	}

	public void degree(String type, String data)
	{
		java.lang.System.setSecurityManager(curManager);
		if(dataframe instanceof TinkerFrame)
		{
			Object degree = ((TinkerFrame)dataframe).degree(type, data);
			String output = "Degrees for  " + data + ":" + degree;
			System.out.println(output);
		}
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	public void eigen(String type, String data)
	{
		java.lang.System.setSecurityManager(curManager);
		if(dataframe instanceof TinkerFrame)
		{
			Object degree = ((TinkerFrame)dataframe).eigen(type, data);
			String output = "Eigen for  " + data + ":" +degree;
			System.out.println(output);
		}
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	public void isOrphan(String type, String data)
	{
		java.lang.System.setSecurityManager(curManager);
		if(dataframe instanceof TinkerFrame)
		{
			boolean orphan = ((TinkerFrame)dataframe).isOrphan(type, data);
			String output = data + "  Orphan? " + orphan;
			System.out.println(output);
		}		
		java.lang.System.setSecurityManager(reactorManager);
	}	
}
