package prerna.test;

/**
* StopWatch is a simple tool to calculate elapsed time
* 
* @author  August Bender
* @version 1.0
* @since   03-23-2015 
* Questions? Email abender@deloitte.com
*/
public class StopWatch {

	long startTime;
	
	public void start(){
		startTime = System.currentTimeMillis();
	}
	
	public long getElapsedSec(){
		long temp = (System.currentTimeMillis() - startTime)/1000;
		return temp;
	}
	
	public long getElapsedMin(){
		long temp = (getElapsedSec() / 60);
		return temp;
	}
	
	public String getElapsedTime(){
		long tempMin = getElapsedMin();
		long tempSec = (getElapsedSec() % 60); 
		String time = tempMin+":"+tempSec;
		return time;
	}
	
}
