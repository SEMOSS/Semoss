package prerna.sablecc2.reactor.mgmt;

import java.io.File;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.sun.jna.Pointer;
import com.sun.jna.platform.win32.Kernel32;

import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.jna.platform.windows.WinNT;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;
import prerna.engine.impl.r.RserveUtil;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class MgmtUtil {
	
	static long previousTime = 0;
    static SystemInfo si = new SystemInfo();
    static OperatingSystem os = si.getOperatingSystem();
    static GlobalMemory gm = si.getHardware().getMemory();

	protected static final Logger logger = LogManager.getLogger(RserveUtil.class);

	private static final String Temp_FOLDER = (DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/" + "R" + "/" + "Temp" + "/").replace('\\', '/');


	public static  void diskUtilizationPerProcess(int pid) {
        /**
         * ByteRead : Returns the number of bytes the process has read from disk.
         * ByteWritten : Returns the number of bytes the process has written to disk.
         */
        OSProcess process;
        process = os.getProcess(pid);
        System.out.println("\nDisk I/O Usage :");
        System.out.println("I/O Reads: "+process.getBytesRead());
        System.out.println("I/O Writes: "+process.getBytesWritten());
    }
	
	
	public static void cpuUtilizationPerProcess(int processId) {
        /**
         * User Time : Returns the number of milliseconds the process has executed in user mode.
         * Kernel Time : Returns the number of milliseconds the process has executed in kernel/system mode.
         */
        SystemInfo systemInfo = new SystemInfo();
        CentralProcessor processor = systemInfo.getHardware().getProcessor();
        int cpuNumber = processor.getLogicalProcessorCount();
        //int processId = systemInfo.getOperatingSystem().getProcessId();
        OSProcess process = systemInfo.getOperatingSystem().getProcess(processId);
        
        long currentTime = process.getKernelTime() + process.getUserTime();
        long timeDifference = currentTime - previousTime;
        double processCpu = (100 * (timeDifference / 5000d)) / cpuNumber;
        previousTime = currentTime;
        System.out.println("\nCPU Usage :");
        System.out.println("CPU : "+(int)processCpu+"%");
    }
	
	public static long memoryUtilizationPerProcess(int pid) {
        /**
         * Resident Size : how much memory is allocated to that process and is in RAM
         */
        OSProcess process;
        
        process = os.getProcess(pid);
        return process.getResidentSetSize();
    }
	
	public static int getProcessID(Process p)
    {
        long result = -1;
        try
        {
            //for windows
            if (p.getClass().getName().equals("java.lang.Win32Process") ||
                   p.getClass().getName().equals("java.lang.ProcessImpl")) 
            {
                Field f = p.getClass().getDeclaredField("handle");
                f.setAccessible(true);              
                long handl = f.getLong(p);
                Kernel32 kernel = Kernel32.INSTANCE;
                WinNT.HANDLE hand = new WinNT.HANDLE();
                hand.setPointer(Pointer.createConstant(handl));
                result = kernel.GetProcessId(hand);
                f.setAccessible(false);
            }
            //for unix based operating systems
            else if (p.getClass().getName().equals("java.lang.UNIXProcess")) 
            {
                Field f = p.getClass().getDeclaredField("pid");
                f.setAccessible(true);
                result = f.getLong(p);
                f.setAccessible(false);
            }
        }
        catch(Exception ex)
        {
            result = -1;
        }
        return new Long(result).intValue();
    }
	
	public static void printChild(int pid)
	{
        List <OSProcess> childProcesses = os.getChildProcesses(pid, null, null, 10);

        for(int childIndex = 0;childIndex < childProcesses.size();childIndex++)
        {
        	System.out.println("Process id " + childProcesses.get(childIndex).getProcessID() + " <> " + childProcesses.get(childIndex).getCommandLine());
        }
	}
	
	public static int findChild(int pid, String name)
	{
        List <OSProcess> childProcesses = os.getChildProcesses(pid, null, null, 10);

        for(int childIndex = 0;childIndex < childProcesses.size();childIndex++)
        {
        	OSProcess op = childProcesses.get(childIndex);
        	String pName = op.getCommandLine();
        	if(pName.contains(name))
        		return op.getProcessID();
        }
        
        return -1;
	}
	
	public static long getFreeMemory()
	{
		long available = gm.getAvailable();
		
		// convert to gigs
		available = available / (1024*1024*1024);
		
		// give a 2GB limit
		available = available -2;
		
        return available;
	}

	public static long getTotalMemory()
	{
        return gm.getTotal();
	}
	
	public static int getPidByPort(int port)
	{
		SecurityManager priorManager = System.getSecurityManager();
		System.setSecurityManager(null);
		File tempFile = new File(Temp_FOLDER + Utility.getRandomString(12) + ".txt");
		try {
			if (SystemUtils.IS_OS_WINDOWS) {

				// Dump the output of netstat to a file
				ProcessBuilder pbNetstat = new ProcessBuilder("netstat", "-ano");
				pbNetstat.redirectOutput(tempFile);
				Process processNetstat = pbNetstat.start();
				processNetstat.waitFor(7L, TimeUnit.SECONDS);
				
				// Parse netstat output to get the PIDs of processes running on Rserve's port
				List<String> lines = FileUtils.readLines(tempFile, "UTF-8");
				List<String> pids = lines.stream()
						.filter(l -> l.contains("LISTENING")) // Only grab processes in LISTENING state
						.map(l -> l.trim().split("\\s+")) // Trim the empty characters and split into rows
						.filter(r -> r[1].contains(":" + port)) // Only use those that are listening on the right port 
						.map(r -> r[4]) // Grab the pid
						.collect(Collectors.toList());
				for (String pid : pids) {
					try {
						return Integer.parseInt(pid.trim());
					} catch (NumberFormatException e) {
						logger.error("pid is not a valid pid");
						logger.error(Constants.STACKTRACE, e);
						throw e;
					}
				}

			} else {
					
				// Dump the output of lsof to a file
				ProcessBuilder pbLsof = new ProcessBuilder("lsof", "-t", "-i:" + port, "-sTCP:LISTEN");
				pbLsof.redirectOutput(tempFile);
				Process processLsof = pbLsof.start();
				processLsof.waitFor(7L, TimeUnit.SECONDS);
				
				// Parse lsof output to get the PIDs of processes (in this case each line is just the pid)
				List<String> lines = FileUtils.readLines(tempFile, "UTF-8");
				for (String pid : lines) {
					try {
						return Integer.parseInt(pid.trim());
					} catch (NumberFormatException e) {
						logger.error("pid is not a valid pid");
						logger.error(Constants.STACKTRACE, e);
						throw e;
					}
				}
			}
		}catch(Exception ignored)
		{}
		finally {
			tempFile.delete();
			
			// Restore the prior security manager
			System.setSecurityManager(priorManager);
		}
		return -1;
	}
	

	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int pid=9696;
		pid = 7672;
		printChild(pid);
		
		getFreeMemory();
		
		/*
        for (int i = 0; i < 10; i++) {
            diskUtilizationPerProcess(pid);
            memoryUtilizationPerProcess(pid);
            Util.sleep(5000);
        }
        */
    }

}
