package prerna.util;

import java.io.File;
import java.io.FilenameFilter;

public class PropFilter implements FilenameFilter
{
	String ext = ".smss";
	@Override
	public boolean accept(File dir, String name) {
		// TODO Auto-generated method stub
		return name.endsWith(ext);
	}
	
}
