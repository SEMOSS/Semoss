package prerna.sablecc2.reactor.storage;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.io.StringBufferInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.sablecc2.PlannerTranslation;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.om.Job;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.PKSLPlanner;
import prerna.util.ArrayUtilityMethods;

public class LoadClient extends AbstractReactor {

	private static final Logger LOGGER = LogManager.getLogger(LoadClient.class.getName());

	//TODO: find a common place to put these
	public static final String STORE_NOUN = "store";
	public static final String ENGINE_NOUN = "engine";
	public static final String CLIENT_NOUN = "client";
	public static final String SCENARIO_NOUN = "scenario";
	public static final String VERSION_NOUN = "version";

	@Override
	public void In() {
        curNoun("all");
	}

	@Override
	public Object Out() {
		// TODO Auto-generated method stub
		return null;
	}
	
	 @Override
     public NounMetadata execute()
     {
     
		 //Create the pkslQueries
		 List<String> pkslQueries = createPkslQueries();
		 
		 //Run them through a PlannerTranslation
		 //Grab the planner
		 PKSLPlanner newPlan = createPlanner(pkslQueries);
		 
		 //replace this planner with generated planner
         this.planner = newPlan;
            
       return null;
     }
	
	private PKSLPlanner createPlanner(List<String> pkslQueries) {
		PlannerTranslation t = new PlannerTranslation();
        
        for(String pkslQuery : pkslQueries) {
        	Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new StringBufferInputStream(pkslQuery)))));
        	Start tree;
			try {
				tree = p.parse();
				tree.apply(t);
			} catch (ParserException | LexerException | IOException e) {
				e.printStackTrace();
			}
        	
        }
        return t.planner;
	}
	
	 //Generate out list of pksl queries based on the data
	private List<String> createPkslQueries() {
		List<String> pkslQueries = new ArrayList<>();
		IRawSelectWrapper iterator = (IRawSelectWrapper)getIterator();
        String[] headers = iterator.getDisplayVariables();
        int fieldIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(headers, "FiedName");
        int formIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(headers, "FormName");
        int valueIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(headers, "Value_1");
        while(iterator.hasNext()) {
          
          IHeadersDataRow nextData = iterator.next();
          Object[] values = nextData.getValues();
          Object field = values[fieldIndex];
          Object form = values[formIndex];
          Object value = values[valueIndex];
          
          String nextPksl = generatePkslQuery(field, form, value, null);
          pkslQueries.add(nextPksl);
        }
        
        return pkslQueries;
	}
	
	private String generatePkslQuery(Object field, Object form, Object value, Object type) {
        return form+"__"+field+" = "+value+";";
    }
	
	private Iterator getIterator() {
		List<Object> jobs = this.curRow.getColumnsOfType(PkslDataTypes.JOB);
		if(jobs != null && jobs.size() > 0) {
			Job job = (Job)jobs.get(0);
			return job.getIterator();
		}

		Job job = (Job)this.getNounStore().getNoun(PkslDataTypes.JOB.toString()).get(0);
		return job.getIterator();
	}
	
	//method to see the structure of the plan
	private void printPlan() {
		
	}


}
