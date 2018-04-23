package prerna.sablecc2;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.Insight;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ConstantDataTask;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.reactor.frame.FrameFactory;

public class PixelStreamUtility {

	private PixelStreamUtility() {
		
	}
	
	
	public static void processPixelRunner(PrintStream ps, Gson gson, PixelRunner runner) {
		// get the values we need from the runner
		Insight in = runner.getInsight();
		List<NounMetadata> resultList = runner.getResults();
		// get the expression which created the return
		// this matches with the above by index
		List<String> pixelStrings = runner.getPixelExpressions();
		List<Boolean> isMeta = runner.isMeta();
		Map<String, String> encodedTextToOriginal = runner.getEncodedTextToOriginal();
		boolean invalidSyntax = runner.isInvalidSyntax();
		
		// start of the map
		// and the insight id
		ps.print("{\"insightID\":\"" + in.getInsightId() + "\",");
		ps.flush();
		
		// now flush array of pixel returns
		ps.println("\"pixelReturn\":[");
		int size = pixelStrings.size();
		
		// THIS IS BECAUSE WE APPEND THE JOB PIXEL
		// BUT FE DOENS'T RESPOND TO IT AND NEED TO REMOVE IT
		// HOWEVER, IF THE SIZE IS JUST 1, IT MEANS THAT THERE WAS
		// AN ERROR THAT OCCURED
		int startIndex = 1;
		if(size == 1) {
			startIndex = 0;
		}
		for (int i = startIndex; i < size; i++) {
			NounMetadata noun = resultList.get(i);
			String expression = pixelStrings.get(i);
			expression = PixelUtility.recreateOriginalPixelExpression(expression, encodedTextToOriginal);
			boolean meta = isMeta.get(i);
			processNounMetadata(ps, gson, noun, expression, meta);
			
			// update the pixel list to say this is routine is valid
			// TODO: need to set this inside the translation directly!!!
			if (!meta && !invalidSyntax) {
				// update the insight recipe
				in.getPixelRecipe().add(expression);
			}
			
			// add a comma for the next item in the list
			if( (i+1) != size) {
				ps.print(",");
				ps.flush();
			}
		}
		
		// now close of the array and the map
		ps.print("]}");
		ps.flush();
	}
	
	
	/**
	 * Process the noun metadata for consumption on the FE
	 * @param noun
	 * @return
	 */
	public static void processNounMetadata(PrintStream ps, Gson gson, NounMetadata noun, String expression, Boolean isMeta) {
		ps.print("{");
		
		// add expression if there
		if(expression != null) {
			ps.print("\"pixelExpression\":" + gson.toJson(expression) + ",");
		}
		// add is meta if there
		if(isMeta != null) {
			ps.print("\"isMeta\":" + isMeta + ",");
		}
		
		PixelDataType nounT = noun.getNounType();
		if(nounT == PixelDataType.FRAME) {
			// if we have a frame
			// return the table name of the frame
			// FE needs this to create proper QS
			// this has no meaning for graphs
			Map<String, String> frameData = new HashMap<String, String>();
			ITableDataFrame frame = (ITableDataFrame) noun.getValue();
			frameData.put("type", FrameFactory.getFrameType(frame));
			String name = frame.getTableName();
			if(name != null) {
				frameData.put("name", name);
			}
			
			ps.print("\"output\":");
			ps.print(gson.toJson(frameData));
			ps.print(",\"operationType\":");
			ps.print(gson.toJson(noun.getOpType()));
			
			// add additional outputs
			List<NounMetadata> addReturns = noun.getAdditionalReturn();
			int numOutputs = addReturns.size();
			if(numOutputs > 0) {
				ps.print(",\"additionalOutput\":[");
				for(int i = 0; i < numOutputs; i++) {
					processNounMetadata(ps, gson, addReturns.get(i), null, null);
				}
				ps.print("]");
			}
			
		} else if(nounT == PixelDataType.CODE || nounT == PixelDataType.TASK_LIST) {
			// code is a tough one to process
			// since many operations could have been performed
			// we need to loop through a set of noun meta datas to output
			List<NounMetadata> codeOutputs = (List<NounMetadata>) noun.getValue();
			int numOutputs = codeOutputs.size();
			if(numOutputs > 0) {
				ps.print("\"output\":[");
				for(int i = 0; i < numOutputs; i++) {
					processNounMetadata(ps, gson, codeOutputs.get(i), null, null);
				}
				ps.print("]");
			}
			ps.print(",\"operationType\":");
			ps.print(gson.toJson(noun.getOpType()));
		} 
		
		else if(nounT == PixelDataType.FORMATTED_DATA_SET) {
			// if we have a task
			// we gotta iterate through it to return the data
			ITask task = (ITask) noun.getValue();
			String formatType = task.getFormatter().getFormatType();
			Map<String, Object> taskMeta = task.getMeta();

			if(formatType.equals("TABLE")) {
				ps.print("\"output\":{");
				if(task instanceof ConstantDataTask) {
					ps.print("\"data\":" + gson.toJson( ((ConstantDataTask) task).getOutputData()));
					ps.flush();
				} else {
					// right now, only grid will work
					ps.print("\"data\":{" );
					
					boolean first = true;
					String[] headers = null;
					while(task.hasNext()) {
						IHeadersDataRow row = task.next();
						// need to set the headers
						if(headers == null) {
							headers = row.getHeaders();
							ps.print("\"values\":[");
						}
						
						if(!first) {
							ps.print(",");
						}
						ps.print(gson.toJson(row.getValues()));
						ps.flush();
						
						first = false;
					}
					// end the values and add the headers
					ps.print("],\"headers\":" + gson.toJson(headers));
					ps.print("}" );
				}
				
				for(String taskMetaKey : taskMeta.keySet()) {
					ps.print(",\"" + taskMetaKey + "\":" + gson.toJson(taskMeta.get(taskMetaKey)));
					ps.flush();
				}
				ps.print(",\"taskId\":\"" + task.getId() + "\"");
				ps.print("}");
				ps.print(",\"operationType\":");
				ps.print(gson.toJson(noun.getOpType()));
				
			} else {
				// TODO
				// THIS IS TEMPORARY UNTIL I FIGURE OUT HOW TO DO 
				// GRAPHS OR OTHER TYPES OF FORMATS
				ps.print("\"output\":");
				ps.print(gson.toJson(task.collect(500, true)));
				ps.flush();
				ps.print(",\"operationType\":");
				ps.print(gson.toJson(noun.getOpType()));
				ps.flush();
			}
		} 
		
		// running a saved insight
		else if(nounT == PixelDataType.PIXEL_RUNNER) {
			PixelRunner runner = (PixelRunner) noun.getValue();
			Insight in = runner.getInsight();
			ps.print("\"output\":{");
			ps.print("\"name\":" + gson.toJson(in.getInsightName()));
			ps.print(",\"core_engine\":" + gson.toJson(in.getEngineName()));
			ps.print(",\"core_engine_id\":" + gson.toJson(in.getRdbmsId()));
			ps.flush();
			ps.print(",\"insightData\":");
			// process the inner recipe
			processPixelRunner(ps, gson, runner);
			ps.print("}");
			ps.print(",\"operationType\":");
			ps.print(gson.toJson(noun.getOpType()));
			ps.flush();
			
			//TODO: account for params!!!!
		}
		
		// everything else is simple
		else {
			ps.print("\"output\":");
			ps.print(gson.toJson(noun.getValue()));
			ps.print(",\"operationType\":");
			ps.print(gson.toJson(noun.getOpType()));
		}

		// close the map
		ps.print("}");
		ps.flush();
	}
	
}
