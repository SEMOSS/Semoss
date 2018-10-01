package prerna.sablecc2.comm;

import java.util.ArrayList;
import java.util.List;

import prerna.om.Insight;
import prerna.sablecc2.PixelRunner;

public class JobThread extends Thread {

	Insight insight = null;
	List<String> pixel = null;
	JobStatus status = JobStatus.CREATED;
	PixelRunner runner = null;
	String jobId;

	// need to make provision for preprocess and post process side effects
	// I will come back to this later

	public JobThread(String jobId)
	{
		this.jobId = jobId;
	}

	public void setStatus(JobStatus status)
	{
		this.status = status;
	}

	@Override
	public void run() {
		try {
			//hold();
			status = JobStatus.INPROGRESS;
			runner = insight.runPixel(pixel);
			//hold();
			status = JobStatus.COMPLETE;
		}catch (Exception ex) {
			ex.printStackTrace();
			status = JobStatus.ERROR;
		}
	}
	
	/*public void hold()
	{
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			System.out.println("And you say ?");
			br.readLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}*/

	public void addPixel(String pixel) {
		if(this.pixel == null) {
			this.pixel = new ArrayList<String>();
		}
		this.pixel.add(pixel);
	}

	public Insight getInsight() {
		return insight;
	}

	public void setInsight(Insight insight) {
		this.insight = insight;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public String getJobId() {
		return this.jobId;
	}

	public PixelRunner getRunner() {
		return runner;
	}

	public String getStatus() {
		return this.status.value;
	}

}
