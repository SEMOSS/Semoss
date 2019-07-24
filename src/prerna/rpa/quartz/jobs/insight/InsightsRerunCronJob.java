package prerna.rpa.quartz.jobs.insight;

import java.util.Vector;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;

import com.ibm.icu.util.StringTokenizer;

import prerna.engine.api.IEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class InsightsRerunCronJob implements org.quartz.InterruptableJob {

	//private static final String ENGINE_NAMES = "ENGINE_NAMES";
	public static final String ENGINES_KEY = InsightsRerunCronJob.class+".engines";
	/*private static final String INSIGHTS_RERUN_CRON_TRIGGER_GROUP = "InsightsRerunCronTriggerGroup";
	private static final String INSIGHTS_RERUN_CRON_TRIGGER = "InsightsRerunCronTrigger";
	private static final String INSIGHTS_RERUN_CRON_JOB_GROUP = "InsightsRerunCronJobGroup";
	private static final String INSIGHTS_RERUN_CRON_JOB = "InsightsRerunCronJob";*/
	private String cronExpression;
	private String engineNames;
	protected String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
	@Override
	public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
		System.out.println("jobExecutionContext :: "+jobExecutionContext);
		engineNames = (String)jobExecutionContext.getMergedJobDataMap().get(ENGINES_KEY);
		if (engineNames == null || engineNames.isEmpty()){
			return;
		}
		StringTokenizer engineInsightRerun = new StringTokenizer(getEngineNames(), ";");
		Vector<Insight> insights = null;
		while(engineInsightRerun.hasMoreElements()){
			String engineName = engineInsightRerun.nextToken();
			// use engine name and then fetch insights
			IEngine engine = Utility.getEngine(engineName);
			Vector<String> resultInsights = null;
			try{
				resultInsights = engine.getInsights();
			} catch(Exception _e){
				if(_e instanceof NullPointerException)
					System.out.println("Engine mentioned in RDF_MAP file DOES NOT exists :: "+_e);
				System.out.println(_e);
			}
			 
			if(resultInsights!=null && !resultInsights.isEmpty())
				insights = ((AbstractEngine)engine).getInsight(resultInsights.toArray(new String[resultInsights.size()]));
			if(insights != null){
				// iterate each insights and re-run each
				for (Insight insight : insights) {
					try{
						//delete insight cache
						//TODO - here CSV cache is not handled 
//						CacheFactory.getInsightCache(CacheFactory.CACHE_TYPE.DB_INSIGHT_CACHE).deleteInsightCache(insight);

						InsightStore.getInstance().put(insight.getInsightId(), insight);
						// TODO InsightCreateRunner does not exist
						/*
						InsightCreateRunner run = new InsightCreateRunner(insight);
						Map<String, Object> insightOutput = run.runSavedRecipe();
						
						if(!(insight.getDataMaker() instanceof Dashboard)) {
							String saveFileLocation = CacheFactory.getInsightCache(CacheFactory.CACHE_TYPE.DB_INSIGHT_CACHE).cacheInsight(insight, insightOutput);
							if(saveFileLocation != null) {
								saveFileLocation = saveFileLocation + "_Solr.txt";
								File solrFile = new File(saveFileLocation);
								String solrId = SolrIndexEngine.getSolrIdFromInsightEngineId(insight.getEngineName(), insight.getRdbmsId());
								SolrDocumentExportWriter writer = new SolrDocumentExportWriter(solrFile);
								writer.writeSolrDocument(SolrIndexEngine.getInstance().getInsight(solrId));
								writer.closeExport();
							}
						}
						*/
					}catch(Exception e){
						System.out.println("exception Occured ::"+e);
						throw new JobExecutionException(e);
					}
				}
			}
		}
	}

	public void setCronExpression(String cronExpression) {
		this.cronExpression = cronExpression;
	}

	public String getCronExpression() {
		return cronExpression;
	}
	public void setEngineNames(String engineNames) {
		this.engineNames = engineNames;
	}
	public String getEngineNames() {
		return engineNames;
	}
	/*private boolean deleteInsightDir(File insightDir){
		boolean d = false;
	    File[] l = insightDir.listFiles();
	    if(l != null)
	    	for (File f : l){
	    		if (f.isDirectory())
	    			deleteInsightDir(f);
	    		else
	    			d = f.delete();
	    	}
	    d = insightDir.delete();
	    return d;
	    
	}*/

	@Override
	public void interrupt() throws UnableToInterruptJobException {
				
	}
}
