package prerna.sablecc2;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;

import org.apache.log4j.Logger;

import com.google.gson.Gson;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.Insight;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.AbstractTask;
import prerna.sablecc2.om.task.ConstantDataTask;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.reactor.export.GraphFormatter;
import prerna.sablecc2.reactor.frame.FrameFactory;
import prerna.util.gson.GsonUtility;
import prerna.util.insight.InsightUtility;

public class PixelStreamUtility {

	private static final String CLASS_NAME = PixelStreamUtility.class.getName();
	private static final Logger LOGGER = Logger.getLogger(CLASS_NAME);

	private static Gson getDefaultGson() {
		return GsonUtility.getDefaultGson();
	}
	
	/**
	 * Collect pixel data from the runner
	 * @param runner
	 * @return
	 */
	public static StreamingOutput collectPixelData(PixelRunner runner) {
		// get the default gson object
		Gson gson = getDefaultGson();

		// now process everything
		try {
			return new StreamingOutput() {
				@Override
				public void write(OutputStream outputStream) throws IOException, WebApplicationException {
					try(PrintStream ps = new PrintStream(outputStream)) {
						// we want to ignore the first index since it will be a job
						processPixelRunner(ps, gson, runner, true);
						ps.close();
					}
				}};
		} catch (Exception e) {
			LOGGER.error("Failed to write object to stream");
		}
		return null;
	}
	
	/**
	 * Collect pixel data from the runner
	 * @param runner
	 * @return
	 */
	public static File writePixelData(PixelRunner runner, File fileToWrite, boolean ignoreFirst) {
		// get the default gson object
		Gson gson = getDefaultGson();

		// now process everything
		FileOutputStream fos = null;
		try {
			StreamingOutput output = new StreamingOutput() {
				PrintStream ps = null;
				@Override
				public void write(OutputStream outputStream) throws IOException, WebApplicationException {
					try {
						ps = new PrintStream(outputStream);
						// we want to ignore the first index since it will be a job
						processPixelRunner(ps, gson, runner, ignoreFirst);
					} catch(Exception e) {
						// ugh... this is unfortunate
					} finally {
						if(ps != null) {
							ps.close();
						}
					}
				}};
			fos = new FileOutputStream(fileToWrite);
			output.write(fos);
		} catch (Exception e) {
			LOGGER.error("Failed to write object to stream");
		} finally {
			try {
				fos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return fileToWrite;
	}

	private static void processPixelRunner(PrintStream ps, Gson gson, PixelRunner runner, boolean ignoreFirst) {
		// get the values we need from the runner
		Insight in = runner.getInsight();
		List<NounMetadata> resultList = runner.getResults();
		// get the expression which created the return
		// this matches with the above by index
		List<String> pixelStrings = runner.getPixelExpressions();
		List<Boolean> isMeta = runner.isMeta();

		// start of the map
		// and the insight id
		ps.print("{\"insightID\":\"" + in.getInsightId() + "\",");
		ps.flush();

		// now flush array of pixel returns
		ps.print("\"pixelReturn\":[");
		int size = pixelStrings.size();

		// THIS IS BECAUSE WE APPEND THE JOB PIXEL
		// BUT FE DOENS'T RESPOND TO IT AND NEED TO REMOVE IT
		// HOWEVER, IF THE SIZE IS JUST 1, IT MEANS THAT THERE WAS
		// AN ERROR THAT OCCURED
		// but when we run a saved insight within a pixel
		// we do not want to shift the index
		int startIndex = 0;
		if(ignoreFirst) {
			startIndex = 1;
		}
		if(size == 1) {
			startIndex = 0;
		}
		for (int i = startIndex; i < size; i++) {
			NounMetadata noun = resultList.get(i);
			String expression = pixelStrings.get(i);
			boolean meta = isMeta.get(i);
			processNounMetadata(in, ps, gson, noun, expression, meta);

			// add a comma for the next item in the list
			if( (i+1) != size) {
				ps.print(",");
				ps.flush();
			}
		}

		// now close of the array and the map
		ps.print("]}");
		ps.flush();
		
		// help java do garbage cleaning
		resultList.clear();
		pixelStrings.clear();
		resultList = null;
		pixelStrings = null;
		runner = null;
	}

	/**
	 * Process the noun metadata for consumption on the FE
	 * @param noun
	 * @return
	 */
	private static void processNounMetadata(Insight in, PrintStream ps, Gson gson, NounMetadata noun, String expression, Boolean isMeta) {
		PixelDataType nounT = noun.getNounType();

		// returning a cached insight
		if(nounT == PixelDataType.CACHED_PIXEL_RUNNER) {
			List<Object> pixelReturn = (List<Object>) noun.getValue();
			for(int i = 0; i < pixelReturn.size(); i++) {
				if(i > 0) {
					ps.print(",");
				}
				ps.print(gson.toJson(pixelReturn.get(i)));
			}
			return;
		}
		
		ps.print("{");

		// add expression if there
		if(expression != null) {
			ps.print("\"pixelExpression\":" + gson.toJson(expression) + ",");
		}
		// add is meta if there
		if(isMeta != null) {
			ps.print("\"isMeta\":" + isMeta + ",");
		}

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

		} else if(nounT == PixelDataType.CODE || nounT == PixelDataType.TASK_LIST || nounT == PixelDataType.VECTOR) {
			// code is a tough one to process
			// since many operations could have been performed
			// we need to loop through a set of noun meta datas to output
			List<NounMetadata> codeOutputs = (List<NounMetadata>) noun.getValue();
			int numOutputs = codeOutputs.size();
			if(numOutputs > 0) {
				ps.print("\"output\":[");
				for(int i = 0; i < numOutputs; i++) {
					if(i > 0) {
						ps.print(",");
					}
					processNounMetadata(in, ps, gson, codeOutputs.get(i), null, null);
				}
				ps.print("]");
			}
			ps.print(",\"operationType\":");
			ps.print(gson.toJson(noun.getOpType()));

		} else if(nounT == PixelDataType.TASK) {
			// if we have a task
			// we gotta iterate through it to return the data
			ITask task = (ITask) noun.getValue();
			ps.print("\"output\":{");
			ps.print("\"taskId\":\"" + task.getId() + "\"");
			ps.print("}");
			ps.print(",\"operationType\":");
			ps.print(gson.toJson(noun.getOpType()));
	
		} else if(nounT == PixelDataType.FORMATTED_DATA_SET) {
			Object value = noun.getValue();
			if(value instanceof ITask) {
				// if we have a task
				// we gotta iterate through it to return the data
				ITask task = (ITask) noun.getValue();
				int numCollect = task.getNumCollect();
				boolean collectAll = numCollect == -1;
				String formatType = task.getFormatter().getFormatType();

				if(task instanceof ConstantDataTask) {
					ps.print("\"output\":{");
					ps.print("\"data\":" + gson.toJson( ((ConstantDataTask) task).getOutputData()));
					ps.flush();
				} else if(formatType.equals("TABLE")) {
					// right now, only grid will work
					boolean first = true;
					String[] headers = null;
					String[] rawHeaders = null;
					int count = 0;

					// we need to use a try catch
					// in case there is an issue
					// since we can get to this point 
					// without trying to execute or anything
					try {
						// recall, a task is also an iterator!
						while(task.hasNext() && (collectAll || count < numCollect)) {
							IHeadersDataRow row = task.next();
							// need to set the headers
							if(headers == null) {
								headers = row.getHeaders();
								rawHeaders = row.getRawHeaders();
								ps.print("\"output\":{");
								ps.print("\"data\":{" );
								ps.print("\"values\":[");
							}

							if(!first) {
								ps.print(",");
							}
							ps.print(gson.toJson(row.getValues()));
							ps.flush();

							first = false;
							count++;
						}
					} catch(Exception e) {
						// on no, this is not good
						e.printStackTrace();

						// let us send back an error
						ps.print("\"output\":");
						ps.print(gson.toJson(e.getMessage()));
						ps.print(",\"operationType\":");
						ps.print(gson.toJson(new PixelOperationType[]{PixelOperationType.ERROR}));
						// close the map
						ps.print("}");
						ps.flush();
						return;
					}

					// this happens if there is no data to return
					if(first == true) {
						ps.print("\"output\":{");
						ps.print("\"data\":{" );
						ps.print("\"values\":[");
						// try to at least provide the headers
						List<Map<String, Object>> headerInfo = task.getHeaderInfo();
						headers = new String[headerInfo.size()];
						for(int i = 0; i < headers.length; i++) {
							headers[i] = headerInfo.get(i).get("alias") + "";
						}
					}
					// end the values and add the headers
					ps.print("],\"headers\":" + gson.toJson(headers));
					ps.print(",\"rawHeaders\":" + gson.toJson(rawHeaders));
					ps.print("}" );

				} else if(formatType.equals("GRAPH")){
//					// format type is probably graph
//					ps.print("\"output\":{");
//					ps.print("\"data\":" );
//					// this is a map return
//					ps.print(gson.toJson( ((AbstractTask) task).getData()));
					
					// format type is graph
					ps.print("\"output\":{");
					printMapData(ps, (Map<String, Object>) ((AbstractTask) task).getData(), gson);
				}

				// grab the meta and output as well
				Map<String, Object> taskMeta = task.getMeta();
				for(String taskMetaKey : taskMeta.keySet()) {
					ps.print(",\"" + taskMetaKey + "\":" + gson.toJson(taskMeta.get(taskMetaKey)));
					ps.flush();
				}
				
				ps.print(",\"taskId\":\"" + task.getId() + "\"");
				ps.print("}");
				ps.print(",\"operationType\":");
				ps.print(gson.toJson(noun.getOpType()));

			}
			// if we do not have a task
			// we just have data to send
			else {
				// sometimes there is just data to send
				// dont need to do anything special
				ps.print("\"output\":");
				Object obj = noun.getValue();
				if(obj instanceof Map && ((Map) obj).containsKey("type") && ((Map) obj).get("type").equals("GRAPH")) {
					ps.print("{");
					Map mapObj = (Map) obj;
					Map<String, Object> retData = (Map<String, Object>) mapObj.remove("data");
					printMapData(ps, retData, gson);
					// print the rest of the stuff
					for(Object key : mapObj.keySet()) {
						ps.println(",\"" + key + ":" + gson.toJson(mapObj.get(key)));
					}
					ps.print("}");
				} else {
					ps.print(gson.toJson(noun.getValue()));
				}
				ps.print(",\"operationType\":");
				ps.print(gson.toJson(noun.getOpType()));
			}
		}

		// running a saved insight
		else if(nounT == PixelDataType.PIXEL_RUNNER) {
			Map<String, Object> runnerWraper = (Map<String, Object>) noun.getValue();
			PixelRunner runner = (PixelRunner) runnerWraper.get("runner");
			Object params = runnerWraper.get("params");
			List<String> additionalPixels = (List<String>) runnerWraper.get("additionalPixels");

			Insight innerInsight = runner.getInsight();
			ps.print("\"output\":{");
			ps.print("\"name\":" + gson.toJson(innerInsight.getInsightName()));
			ps.print(",\"core_engine\":" + gson.toJson(innerInsight.getEngineId()));
			ps.print(",\"core_engine_id\":" + gson.toJson(innerInsight.getRdbmsId()));
			ps.print(",\"recipe\":" + gson.toJson(innerInsight.getPixelRecipe()));
			ps.print(",\"params\":" + gson.toJson(params));
			ps.print(",\"additionalPixels\":" + gson.toJson(additionalPixels));
			ps.flush();
			ps.print(",\"insightData\":");
			// process the inner recipe
			processPixelRunner(ps, gson, runner, false);
			ps.print("}");
			ps.print(",\"operationType\":");
			ps.print(gson.toJson(noun.getOpType()));
			ps.flush();
		}
		
		// remove variable
		else if(nounT == PixelDataType.REMOVE_VARIABLE) {
			// we only remove variables at the end
			// because the user may want to get the task and then
			// remove the frame right after
			// so we need to remove only at the end
			NounMetadata newNoun = InsightUtility.removeVaraible(in.getVarStore(), noun.getValue().toString());
			ps.print("\"output\":");
			ps.print(gson.toJson(newNoun.getValue()));
			ps.print(",\"operationType\":");
			ps.print(gson.toJson(newNoun.getOpType()));
			
		}
		
		// remove insight 
		else if(nounT == PixelDataType.DROP_INSIGHT) {
			NounMetadata newNoun = InsightUtility.dropInsight(in);
			ps.print("\"output\":");
			ps.print(gson.toJson(newNoun.getValue()));
			ps.print(",\"operationType\":");
			ps.print(gson.toJson(newNoun.getOpType()));
		}
		
		else if(nounT == PixelDataType.REMOVE_TASK) {
			// we only remove variables at the end
			// because the user may want to get the task and then
			// remove the frame right after
			// so we need to remove only at the end
			ITask task = InsightUtility.removeTask(in, noun.getValue().toString());
			ps.print("\"output\":{");
			if(task == null) {
				ps.print("\"taskId\":\"Could not find task id = " + noun.getValue().toString() + "\"");
				ps.print("}");
				ps.print(",\"operationType\":");
				ps.print(gson.toJson(new PixelOperationType[]{PixelOperationType.ERROR}));
			} else {
				ps.print("\"taskId\":\"" + task.getId() + "\"");
				ps.print("}");
				ps.print(",\"operationType\":");
				ps.print(gson.toJson(noun.getOpType()));
			}
		}

		// everything else is simple
		else {
			ps.print("\"output\":");
			ps.print(gson.toJson(noun.getValue()));
			ps.print(",\"operationType\":");
			ps.print(gson.toJson(noun.getOpType()));
		}
		
		// add additional outputs
		List<NounMetadata> addReturns = noun.getAdditionalReturn();
		int numOutputs = addReturns.size();
		if(numOutputs > 0) {
			ps.print(",\"additionalOutput\":[");
			for(int i = 0; i < numOutputs; i++) {
				if(i > 0) {
					ps.print(",");
				}
				processNounMetadata(in, ps, gson, addReturns.get(i), null, null);
			}
			ps.print("]");
		}

		// close the map
		ps.print("}");
		ps.flush();
	}
	
	/**
	 * Logic to more efficiently print out the map formatted data 
	 * @param ps
	 * @param retData
	 * @param gson
	 */
	private static void printMapData(PrintStream ps, Map<String, Object> retData, Gson gson) {
		ps.print("\"data\":" );
		// this is a map return
		ps.print("{\"" + GraphFormatter.GRAPH_META + "\":" + gson.toJson(retData.get(GraphFormatter.GRAPH_META)));
		ps.print(", \"" + GraphFormatter.NODES + "\":[");

		List<Object> nodeList = (List<Object>) retData.get(GraphFormatter.NODES);
		// print first node
		if(!nodeList.isEmpty()) {
			ps.print(gson.toJson(nodeList.remove(0)));
		}
		// print rest of nodes
		nodeList.stream().forEach(node -> ps.print("," + gson.toJson(node)));
		ps.print("], \"" + GraphFormatter.EDGES + "\":[");
		List<Object> edgeList = (List<Object>) retData.get(GraphFormatter.EDGES);
		// print first node
		if(!edgeList.isEmpty()) {
			ps.print(gson.toJson(edgeList.remove(0)));
		}
		// print rest of nodes
		edgeList.stream().forEach(edge -> ps.print("," + gson.toJson(edge)));
		ps.print("]}");
	}
	
}
