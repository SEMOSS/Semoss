package prerna.sablecc2.reactor.export;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Vector;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.sablecc2.om.Job;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.AbstractReactor;

/**
 * 
 * This class is responsible for collecting data from a job and returning it
 *
 */
public class CollectReactor extends AbstractReactor{

	@Override
	public void In() {
		curNoun("all");
	}

	@Override
	public Object Out() {
		return parentReactor;
	}

	public NounMetadata execute() {
		Job job = getJob();
		int collectThisMany = getTotalToCollect();
		
		Object data = job.collect(collectThisMany);
		this.planner.addProperty("DATA", "DATA", data);//this is the property that translation looks for when grabbing the Response
		NounMetadata result = new NounMetadata(data, PkslDataTypes.FORMATTED_DATA_SET);
		
		
//		Gson gson = new GsonBuilder().setPrettyPrinting().create();
//		String fileName = "C:\\Workspace\\Semoss_Dev\\exampleOutput.txt";
//		if(new File(fileName).exists()) {
//			new File(fileName).delete();
//		}
//		BufferedWriter bw = null;
//		FileWriter fw = null;
//		try {
//			fw = new FileWriter(fileName);
//			bw = new BufferedWriter(fw);
//			bw.write(gson.toJson(result.getValue()));
//		} catch (IOException e1) {
//			e1.printStackTrace();
//		} finally {
//			try {
//				if(bw != null) {
//					bw.close();
//				}
//				if(fw != null) {
//					fw.close();
//				}
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
		
		return result;
	}
	
	//This gets the Job collect reactor needs to collect from
	private Job getJob() {
		Job job;
		
		List<Object> jobs = curRow.getColumnsOfType(PkslDataTypes.JOB);
		
		//if we don't have jobs in the curRow, check if it exists in genrow under the key job
		if(jobs == null || jobs.size() == 0) {
			job = (Job) getNounStore().getNoun(PkslDataTypes.JOB.toString()).get(0);
		} else {
			job = (Job) curRow.getColumnsOfType(PkslDataTypes.JOB).get(0);
		}
		return job;
	}
	
	//returns how much do we need to collect
	private int getTotalToCollect() {
		Number collectThisMany = (Number) curRow.getColumnsOfType(PkslDataTypes.CONST_DECIMAL).get(0);
		return collectThisMany.intValue();
	}
	
	@Override
	public List<NounMetadata> getOutputs() {
		
		List<NounMetadata> outputs = super.getOutputs();
		if(outputs != null) return outputs;
		
		outputs = new Vector<NounMetadata>();
		NounMetadata output = new NounMetadata(this.signature, PkslDataTypes.FORMATTED_DATA_SET);
		outputs.add(output);
		return outputs;
	}
}
