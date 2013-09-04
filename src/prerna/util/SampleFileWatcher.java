package prerna.util;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import javax.swing.DefaultListModel;
import javax.swing.JFrame;
import javax.swing.JList;

public class SampleFileWatcher extends AbstractFileWatcher {

	@Override
	public void process(String fileName) {
		// TODO Auto-generated method stub
		try {
			//loadExistingDB();
			// for the sample this will never get called
		}catch(Exception ex)
		{ex.printStackTrace();}
	}
	
	@Override
	public void run()
	{
		System.out.println("Bam bam bam");
		System.out.println("Engine Name is " + engine);
	}

	@Override
	public void loadFirst() {
		// TODO Auto-generated method stub
		
	}
	
}
