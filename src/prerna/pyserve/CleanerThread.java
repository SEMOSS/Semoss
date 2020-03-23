package prerna.pyserve;

import java.io.File;

import org.apache.log4j.LogManager;
import org.codehaus.plexus.util.FileUtils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import net.snowflake.client.jdbc.internal.microsoft.azure.storage.core.Logger;

public class CleanerThread extends Thread{
	
	
	// takes command and executes it
	// quite simple
	public String folder = null;
	
	public static final org.apache.log4j.Logger LOGGER = LogManager.getLogger(CleanerThread.class.getName());
	
	public CleanerThread(String folder)
	{
		this.folder = folder;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		int attempt = 1;
		boolean deleted = false;
		while(attempt < 10 && !deleted)
		{
			try {
				FileUtils.deleteDirectory(folder);
				deleted = true;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				attempt++;
				try {
					Thread.sleep(attempt * 1000);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}		
		
		if(attempt >= 10)
			LOGGER.info("Unable to delete Directory " + folder);
		else
			LOGGER.info("Deleted directory " + folder);
	}

}
