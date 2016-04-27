package prerna.sablecc;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.io.StringBufferInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringEscapeUtils;

import cern.colt.Arrays;
import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc.lexer.Lexer;
import prerna.sablecc.lexer.LexerException;
import prerna.sablecc.node.Start;
import prerna.sablecc.parser.Parser;
import prerna.sablecc.parser.ParserException;

public class PKQLRunner {
	
	enum Status {SUCCESS, ERROR}
	
	private String currentStatus = "success";
	private Object response = "PKQL processing complete";
	private Map<String, Object> feData = new HashMap<String, Object>();

	public HashMap<String, Object> runPKQL(String expression, ITableDataFrame f) {
		
		Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new StringBufferInputStream(expression)), 1024)));
		Start tree;

		try {
			tree = p.parse();
			// Apply the translation.
			tree.apply(new Translation2(f, this));

		} catch (ParserException | LexerException | IOException e) {
			e.printStackTrace();
			currentStatus = "error";
			response = "Invalid PKQL Statement";
		}

		HashMap<String, Object> result = new HashMap<String, Object>();
		if(response instanceof Object[]) {
			StringBuilder builder = new StringBuilder();
			String retResponse = getStringFromObjectArray( (Object[]) response, builder);
			result.put("result", StringEscapeUtils.escapeHtml(retResponse));
		} else if(response instanceof List) {
			StringBuilder builder = new StringBuilder();
			String retResponse = getStringFromList( (List) response, builder);
			result.put("result", StringEscapeUtils.escapeHtml(retResponse));
		} else if(response instanceof double[]) {
			result.put("result", StringEscapeUtils.escapeHtml(Arrays.toString( (double[]) response)));
		} else if(response instanceof int[]) {
			result.put("result", StringEscapeUtils.escapeHtml(Arrays.toString( (int[]) response)));
		} else { 
			result.put("result", StringEscapeUtils.escapeHtml(response + ""));
		}
		result.put("status", currentStatus);
		return result;
	}

	private String getStringFromList(List objList, StringBuilder builder) {
		for(int i = 0; i < objList.size(); i++) {
			Object obj = objList.get(i);
			if(obj instanceof Object[]) {
				getStringFromObjectArray( (Object[]) obj, builder);
			} else if(obj instanceof double[]) {
				builder.append(Arrays.toString((double[]) obj));
			} else if(obj instanceof int[]) {
				builder.append(Arrays.toString((int[]) obj));
			}
			builder.append("\n");
		}
		
		return builder.toString();
	}
	
	private String getStringFromObjectArray(Object[] objArray, StringBuilder builder) {
		builder.append("[");
		for(int i = 0; i < objArray.length; i++) {
			Object obj = objArray[i];
			if(obj instanceof Object[]) {
				getStringFromObjectArray((Object[]) obj, builder);
			} else {
				if(i == objArray.length-1) {
					if(obj instanceof double[]) {
						builder.append(Arrays.toString((double[]) obj));
					} else if(obj instanceof int[]) {
						builder.append(Arrays.toString((int[]) obj));
					} else {
						builder.append(obj);
					}
				} else {
					// since primitive arrays are stupid in java
					if(obj instanceof double[]) {
						builder.append(Arrays.toString((double[]) obj)).append(", ");
						builder.append("\n");
					} else if(obj instanceof int[]) {
						builder.append(Arrays.toString((int[]) obj)).append(", ");
						builder.append("\n");
					} else {
						builder.append(obj).append(", ");
					}
				}
			}
		}
		builder.append("]");
		
		return builder.toString();
	}
	
	public void setResponse(Object response) {
		this.response = response;
	}
	
	public void setStatus(String currentStatus) {
		this.currentStatus = currentStatus;
	}
	
	public void addFeData(String key, Object value){
		this.feData.put(key, value);
	}
	
	public Map<String, Object> getFeData(){
		return this.feData;
	}
}
