package prerna.tcp;

import prerna.test.InterimReactorTest;

public class WorkerThread implements Runnable {

	SocketServerHandler ssh = null;
	PayloadStruct inputStruct = null;

	public WorkerThread(SocketServerHandler ssh, PayloadStruct inputStruct)
	{
		this.ssh = ssh;
		this.inputStruct = inputStruct;
	}
	
	@Override
	public void run() {
		System.err.println(">");
		// TODO Auto-generated method stub
		PayloadStruct output = ssh.getFinalOutput((PayloadStruct)inputStruct);
		if(output != null)
			ssh.writeResponse(output);
	}

}
