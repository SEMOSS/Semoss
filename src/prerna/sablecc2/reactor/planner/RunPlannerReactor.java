package prerna.sablecc2.reactor.planner;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.util.List;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.sablecc2.Translation;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;
import prerna.sablecc2.reactor.storage.MapStore;

public class RunPlannerReactor extends AbstractPlannerReactor {

	@Override
	public void In() {
		curNoun("all");
	}

	@Override
	public Object Out() {
		return parentReactor;
	}
	
	@Override
	public NounMetadata execute()
	{
		long start = System.currentTimeMillis();
		
		// we use this just for debugging
		// this will get undefined variables and set them to 0
		// ideally, we should never have to do this...
		List<String> pksls = getUndefinedVariablesPksls(this.planner);
		
		// get the list of the root vertices
		// these are the vertices we can run right away
		// and are the starting point for the plan execution
		Set<Vertex> rootVertices = getRootPksls(this.planner);
		// using the root vertices
		// iterate down all the other vertices and add the signatures
		// for the desired travels in the appropriate order
		// note: this is adding to the list of undefined variables
		// calculated at beginning of class 
		traverseDownstreamVerts(rootVertices, pksls);
		
		Translation translation = new Translation();
		
		String fileName = "C:\\Workspace\\Semoss_Dev\\failedpksls.txt";
		BufferedWriter bw = null;
		FileWriter fw = null;
		
		try {
			fw = new FileWriter(fileName);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		bw = new BufferedWriter(fw);
		int count = 0;
		int total = 0;
		for(String pkslString : pksls) {
			try {
				Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(pkslString.getBytes("UTF-8"))))));
				
				Start tree = p.parse();
				tree.apply(translation);
				bw.write("COMPLETE::: "+pkslString+"\n");
			} catch (ParserException | LexerException | IOException e) {
				count++;
				e.printStackTrace();
				System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
				System.out.println(pkslString);
				try {
					bw.write("PARSE ERROR::::   "+pkslString+"\n");
				} catch (IOException e1) {
					e1.printStackTrace();
				}
//				System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
			} catch(Exception e) {
				count++;
				e.printStackTrace();
				System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
				System.out.println(pkslString);
				try {
					bw.write("EVAL ERROR:::: " + e.getMessage()+"\n");
					bw.write("EVAL ERROR:::: "+pkslString+"\n");
					bw.write("\n");
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
			}
			total++;
		}
		
		try {
			bw.close();
			fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("****************    "+total+"      *************************");
		System.out.println("****************    "+count+"      *************************");
		
		MapStore mapStore = getMapStore();
		Set<String> variables = translation.planner.getVariables();
		for(String variable : variables) {
			mapStore.put(variable, translation.planner.getVariableValue(variable));
		}
		
		long end = System.currentTimeMillis();
		System.out.println("****************    "+(end - start)+"      *************************");
		
		return new NounMetadata(mapStore, PkslDataTypes.IN_MEM_STORE);
	}
	
	
	private MapStore getMapStore() {
		return new MapStore();
	}
}
