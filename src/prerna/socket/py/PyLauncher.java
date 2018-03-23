package prerna.socket.py;

import com.corundumstudio.socketio.AckRequest;
import com.corundumstudio.socketio.Configuration;
import com.corundumstudio.socketio.SocketIOClient;
import com.corundumstudio.socketio.SocketIOServer;
import com.corundumstudio.socketio.listener.ConnectListener;
import com.corundumstudio.socketio.listener.DataListener;

public class PyLauncher {

	private SocketIOServer server;

	// singleton design
	private static PyLauncher singleton;
	
	public static PyLauncher getInstance() {
		if(singleton == null) {
			singleton = new PyLauncher();
		}
		return singleton;
	}

	private PyLauncher() {
		Configuration config = new Configuration();
		config.setHostname("localhost");
		config.setPort(3000);

		this.server = new SocketIOServer(config);
		addListeners();
	}
	
	/**
	 * Add the default listeners to the socket io
	 */
	private void addListeners() {
		this.server.addConnectListener(new ConnectListener(){

			@Override
			public void onConnect(SocketIOClient client) {
				System.out.println("Client Connected.. " + client.getSessionId());
				client.sendEvent("chat message", "welcome from NETTY");
			}

		});

		this.server.addEventListener("chat message", String.class, new DataListener<String>() {
			@Override
			public void onData(SocketIOClient client, String data, AckRequest ackRequest) {
				// broadcast messages to all clients
				System.out.println(client.getSessionId());
				System.out.println(" Data came in.. " + data);
				server.getBroadcastOperations().sendEvent("chat message", data);
			}
		});
	}
	
	/**
	 * Start the server
	 */
	public PyLauncher startServer() {
		server.start();
		return this;
	}
	
	/**
	 * Stop the server
	 */
	public void stopServer() {
		server.stop();
	}

	
	
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////

	
	/**
	 * Main method to start the IO server
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws InterruptedException {
		PyLauncher launcher = PyLauncher.getInstance().startServer();
		Thread.sleep(Integer.MAX_VALUE);
	}


}
