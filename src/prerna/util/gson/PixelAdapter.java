package prerna.util.gson;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.om.Pixel;
import prerna.om.Variable;
import prerna.sablecc2.om.task.options.TaskOptions;

public class PixelAdapter extends AbstractSemossTypeAdapter<Pixel> {
	
	private static final Gson GSON = GsonUtility.getDefaultGson();

	@Override
	public Pixel read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}

		String id = null;
		String pixelString = null;
		String pixelAlias = null;
		String pixelDescription = null;
		boolean meta = false;
		boolean isRefreshPanel = false;
		boolean isCodeExecution = false;
		boolean isUserScript = false;
		String codeExecuted = null;
		Variable.LANGUAGE language = null;
		boolean isFrameTransformation = false;
		boolean isAssignment = false;
		boolean isFileRead = false;
		boolean isSaveDataTransformation = false;
		boolean isSaveDataExport = false;
		boolean isSaveVisualization = false;
		long timeToRun = -1;
		boolean error = false;
		boolean warning = false;
		Map<String, Map<String, Object>> startingFrameHeaders = null;
		Map<String, Map<String, Object>> endingFrameHeaders = null;
//		List<Map<String, List<Map>>> reactorInputs = null;
		Set<String> frameOutputs = new HashSet<>();
		Set<String> frameInputs = new HashSet<>();
		List<TaskOptions> taskOptions = new Vector<>();
		Map<String, Object> positionMap = null;
		List<String> errorMessages = new Vector<>();
		List<String> warningMessages = new Vector<>();
		
		TypeAdapter mapAdapter = GSON.getAdapter(Map.class);
//		TypeAdapter listAdapter = GSON.getAdapter(List.class);

		in.beginObject();
		while(in.hasNext()) {
			String key = in.nextName();
			if(in.peek() == JsonToken.NULL) {
				in.nextNull();
				continue;
			}
			
			if(key.equals("id")) {
				id = in.nextString();
			} else if(key.equals("pixelString")) {
				pixelString = in.nextString();
			} else if(key.equals("pixelAlias")) {
				pixelAlias = in.nextString();
			} else if(key.equals("pixelDescription")) {
				pixelDescription = in.nextString();
			} else if(key.equals("meta")) {
				meta = in.nextBoolean();
			} else if(key.equals("isRefreshPanel")) {
				isRefreshPanel = in.nextBoolean();
			} else if(key.equals("isCodeExecution")) {
				isCodeExecution = in.nextBoolean();
			} else if(key.equals("isUserScript")) {
				isUserScript = in.nextBoolean();
			} else if(key.equals("codeExecuted")) {
				codeExecuted = in.nextString();
			} else if(key.equals("language")) {
				String strLanguage = in.nextString();
				language = Variable.LANGUAGE.valueOf(strLanguage);
			} else if(key.equals("isFrameTransformation")) {
				isFrameTransformation = in.nextBoolean();
			} else if(key.equals("isAssignment")) {
				isAssignment = in.nextBoolean();
			} else if(key.equals("isFileRead")) {
				isFileRead = in.nextBoolean();
			} else if(key.equals("isSaveDataTransformation")) {
				isSaveDataTransformation = in.nextBoolean();
			} else if(key.equals("isSaveDataExport")) {
				isSaveDataExport = in.nextBoolean();
			} else if(key.equals("isSaveVisualization")) {
				isSaveVisualization = in.nextBoolean();
			} else if(key.equals("timeToRun")) {
				timeToRun = in.nextLong();
			} else if(key.equals("errorReturned")) {
				error = in.nextBoolean();
			} else if(key.equals("warningReturned")) {
				warning = in.nextBoolean();
			} else if(key.equals("startingFrameHeaders")) {
				startingFrameHeaders = (Map<String, Map<String, Object>>) mapAdapter.read(in);
			} else if(key.equals("endingFrameHeaders")) {
				endingFrameHeaders = (Map<String, Map<String, Object>>) mapAdapter.read(in);
			} 
//			else if(key.equals("reactorInputs")) {
//				reactorInputs = (List<Map<String, List<Map>>>) listAdapter.read(in);
//			} 
			else if(key.equals("frameInputs")) {
				in.beginArray();
				while(in.hasNext()) {
					frameInputs.add(in.nextString());
				}
				in.endArray();
			} else if(key.equals("frameOutputs")) {
				in.beginArray();
				while(in.hasNext()) {
					frameOutputs.add(in.nextString());
				}
				in.endArray();
			} else if(key.equals("taskOptions")) {
				in.beginArray();
				TaskOptionsAdapter tAdapter = new TaskOptionsAdapter();
				while(in.hasNext()) {
					taskOptions.add(tAdapter.read(in));
				}
				in.endArray();
			} else if(key.equals("positionMap")) {
				positionMap = (Map<String, Object>) mapAdapter.read(in);
			} else if(key.equals("errorMessages")) {
				in.beginArray();
				while(in.hasNext()) {
					errorMessages.add(in.nextString());
				}
				in.endArray();
			} else if(key.equals("warningMessages")) {
				in.beginArray();
				while(in.hasNext()) {
					warningMessages.add(in.nextString());
				}
				in.endArray();
			} 
		}
		in.endObject();

		Pixel pixel = new Pixel(id, pixelString);
		pixel.setPixelAlias(pixelAlias);
		pixel.setPixelDescription(pixelDescription);
		pixel.setMeta(meta);
		pixel.setRefreshPanel(isRefreshPanel);
		if(isCodeExecution) {
			pixel.setCodeDetails(isCodeExecution, codeExecuted, language, isUserScript);
		}
		pixel.setFrameTransformation(isFrameTransformation);
		pixel.setAssignment(isAssignment);
		pixel.setFileRead(isFileRead);
		// force recipe save
		pixel.setSaveDataTransformation(isSaveDataTransformation);
		pixel.setSaveDataExport(isSaveDataExport);
		pixel.setSaveVisualization(isSaveVisualization);
		// time to run
		pixel.setTimeToRun(timeToRun);
		// error messages
		pixel.setReturnedError(error);
		pixel.setReturnedWarning(warning);
		pixel.setErrorMessages(errorMessages);
		pixel.setWarningMessages(warningMessages);
		pixel.setStartingFrameHeaders(startingFrameHeaders);
		pixel.setEndingFrameHeaders(endingFrameHeaders);
//		pixel.setReactorInputs(reactorInputs);
		pixel.setFrameInputs(frameInputs);
		pixel.setFrameOutputs(frameOutputs);
		pixel.setTaskOptions(taskOptions);
		pixel.setPositionMap(positionMap);
		return pixel;
	}
	
	@Override
	public void write(JsonWriter out, Pixel value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}

		out.beginObject();
		// id
		out.name("id");
		out.value(value.getId());
		// pixel
		out.name("pixelString");
		out.value(value.getPixelString());
		// alias
		out.name("pixelAlias");
		out.value(value.getPixelAlias());
		// description
		out.name("pixelDescription");
		out.value(value.getPixelDescription());
		// meta
		out.name("meta");
		out.value(value.isMeta());
		// isRefreshPanel
		out.name("isRefreshPanel");
		out.value(value.isRefreshPanel());
		// isCodeExecution
		out.name("isCodeExecution");
		out.value(value.isCodeExecution());
		// isCodeExecution
		out.name("isUserScript");
		out.value(value.isUserScript());
		// codeExecuted
		out.name("codeExecuted");
		out.value(value.getCodeExecuted());
		// language
		out.name("language");
		if(value.getLanguage() == null) {
			out.nullValue();
		} else {
			out.value(value.getLanguage().name());
		}
		// isFrameTransformation
		out.name("isFrameTransformation");
		out.value(value.isFrameTransformation());
		// isAssignment
		out.name("isAssignment");
		out.value(value.isAssignment());
		// isFileRead
		out.name("isFileRead");
		out.value(value.isFileRead());
		// isForceSave
		out.name("isSaveDataTransformation");
		out.value(value.isSaveDataTransformation());
		out.name("isSaveDataExport");
		out.value(value.isSaveDataExport());
		out.name("isSaveVisualization");
		out.value(value.isSaveVisualization());
		out.name("timeToRun");
		out.value(value.getTimeToRun());
		// error
		out.name("errorReturned");
		out.value(value.isReturnedError());
		// warning
		out.name("warningReturned");
		out.value(value.isReturnedWarning());
		// starting headers
		out.name("startingFrameHeaders");
		if(value.getStartingFrameHeaders() != null) {
			TypeAdapter adapter = GSON.getAdapter(value.getStartingFrameHeaders().getClass());
			adapter.write(out, value.getStartingFrameHeaders());
		} else {
			out.nullValue();
		}
		// end headers
		out.name("endingFrameHeaders");
		if(value.getEndingFrameHeaders() != null) {
			TypeAdapter adapter = GSON.getAdapter(value.getEndingFrameHeaders().getClass());
			adapter.write(out, value.getEndingFrameHeaders());
		} else {
			out.nullValue();
		}
//		// reactor inputs
//		out.name("reactorInputs");
//		if(value.getReactorInputs() != null) {
//			TypeAdapter adapter = GSON.getAdapter(value.getReactorInputs().getClass());
//			adapter.write(out, value.getReactorInputs());
//		} else {
//			out.nullValue();
//		}
		// frame inputs
		out.name("frameInputs");
		out.beginArray();
		for(String frameName : value.getFrameInputs()) {
			out.value(frameName);
		}
		out.endArray();
		// frame outputs
		out.name("frameOutputs");
		out.beginArray();
		for(String frameName : value.getFrameOutputs()) {
			out.value(frameName);
		}
		out.endArray();
		// task options
		out.name("taskOptions");
		out.beginArray();
		{
			TaskOptionsAdapter tAdapter = new TaskOptionsAdapter();
			for(TaskOptions tOptions : value.getTaskOptions()) {
				tAdapter.write(out, tOptions);
			}
		}
		out.endArray();
		// position map
		out.name("positionMap");
		if(value.getPositionMap() != null) {
			TypeAdapter adapter = GSON.getAdapter(value.getPositionMap().getClass());
			adapter.write(out, value.getPositionMap());
		} else {
			out.nullValue();
		
		}
		// error messages
		out.name("errorMessages");
		if(value.getErrorMessages() != null) {
			out.beginArray();
			for(String errorM : value.getErrorMessages()) {
				out.value(errorM);
			}
			out.endArray();
		} else {
			out.nullValue();
		}
		// warning messages
		out.name("warningMessages");
		if(value.getWarningMessages() != null) {
			out.beginArray();
			for(String warningM : value.getWarningMessages()) {
				out.value(warningM);
			}
			out.endArray();
		} else {
			out.nullValue();
		}
		out.endObject();
	}
	
}
