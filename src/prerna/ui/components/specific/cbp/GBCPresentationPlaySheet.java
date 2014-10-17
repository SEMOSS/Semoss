package prerna.ui.components.specific.cbp;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.Insets;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import javax.swing.JButton;
import javax.swing.JDesktopPane;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;

import com.bigdata.rdf.model.BigdataLiteralImpl;

import prerna.om.GraphDataModel;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.rdf.util.StatementCollector;
import prerna.ui.components.ChartControlPanel;
import prerna.ui.components.RDFEngineHelper;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.AbstractRDFPlaySheet;
import prerna.ui.components.playsheets.BasicProcessingPlaySheet;
import prerna.ui.components.playsheets.BrowserPlaySheet;
import prerna.ui.helpers.PlaysheetCreateRunner;
import prerna.ui.main.listener.impl.ColumnChartGroupedStackedListener;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class GBCPresentationPlaySheet extends AbstractRDFPlaySheet{

	private static final Logger logger = LogManager.getLogger(GBCPresentationPlaySheet.class.getName());
	Properties presProps = new Properties();
	String propsKey;
	IPlaySheet realPlaySheet;
	
	public GBCPresentationPlaySheet() 
	{
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String fileName = workingDir + "/GBCPresentation.properties";
		try {
			presProps.load(new FileInputStream(fileName));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void createData()
	{		
		// this will do everything for the real play sheet

		// now that the query is set lets create the playsheet and set those things
		String playSheetClassName = presProps.getProperty(propsKey + "_LAYOUT");
		try {
			this.realPlaySheet = (IPlaySheet) Class.forName(playSheetClassName).getConstructor(null).newInstance(null);
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.realPlaySheet.setTitle(this.title);
		this.realPlaySheet.setRDFEngine(this.engine);
		this.realPlaySheet.setQuestionID(this.propsKey);
		this.realPlaySheet.setQuery(this.query);
		JDesktopPane pane = (JDesktopPane) DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
		this.realPlaySheet.setJDesktopPane(pane);

		// cannot use the create runner because i don't know if it is web or not at this point
		this.realPlaySheet.createData();
		this.realPlaySheet.runAnalytics();
	}
	
	@Override
	public void setQuery(String query1) {
		this.propsKey = query1;
		//query must be specified on the prop sheet or it can be a pointer to a generic query key
		this.query = presProps.getProperty(propsKey+"_QUERY");
		if(presProps.getProperty(query) != null)
			query = presProps.getProperty(query);
		
		//for each param see if defined specifically otherwise get the value for the whole presentation
		Hashtable<String, String> filledParams = new Hashtable<String, String>();
		Hashtable<String, String> params = Utility.getParams(this.query);
		for (String param : params.keySet()){
			String paramKey = param.substring(0, param.indexOf("-"));
			String paramValue = presProps.getProperty(propsKey+"_"+paramKey);
			if(paramValue == null)
				paramValue = presProps.getProperty(paramKey);
			filledParams.put(param, paramValue);
			logger.info("filling param " + paramKey + " with " + paramValue);
		}
		
		this.query = Utility.fillParam(this.query, filledParams);
		logger.info("filled final query : " + this.query);
		
		
	}
	
	@Override
	public void createView(){
		this.realPlaySheet.createView();
	}

	@Override
	public void refineView() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void overlayView() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void runAnalytics() {
		// TODO Auto-generated method stub
		
	}
	
	public Object getData(){
		return this.realPlaySheet.getData();
	}
	
}
