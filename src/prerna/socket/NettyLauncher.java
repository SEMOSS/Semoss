//package prerna.socket;
//
//import java.util.Hashtable;
//import java.util.List;
//
//import com.corundumstudio.socketio.AckRequest;
//import com.corundumstudio.socketio.Configuration;
//import com.corundumstudio.socketio.SocketIOClient;
//import com.corundumstudio.socketio.SocketIOServer;
//import com.corundumstudio.socketio.listener.ConnectListener;
//import com.corundumstudio.socketio.listener.DataListener;
//
//public class NettyLauncher {
//
//	// overall server
//	private SocketIOServer server;
//
//	// keep track of the client
//	// TODO: for now, just keep track of the type
//	private Hashtable<String, SocketIOClient> clientHash;
//	
//	// singleton design
//	private static NettyLauncher singleton;
//	
//	public static NettyLauncher getInstance() {
//		if(singleton == null) {
//			singleton = new NettyLauncher();
//		}
//		return singleton;
//	}
//
//	private NettyLauncher() {
//		Configuration config = new Configuration();
//		config.setHostname("localhost");
//		config.setPort(3000);
//		config.setMaxFramePayloadLength(Integer.MAX_VALUE);
//		this.server = new SocketIOServer(config);
//		addListeners();
//	}
//	
//	/**
//	 * Add the default listeners to the socket io
//	 */
//	private void addListeners() {
//		this.server.addConnectListener(new ConnectListener(){
//
//			@Override
//			public void onConnect(SocketIOClient client) {
//				System.out.println("Client Connected.. " + client.getSessionId());
//				client.sendEvent("hello", "welcome from NETTY");
//			}
//
//		});
//
//		this.server.addEventListener("chat message", String.class, new DataListener<String>() {
//			@Override
//			public void onData(SocketIOClient client, String data, AckRequest ackRequest) {
//				// broadcast messages to all clients
//				System.out.println(client.getSessionId());
//				System.out.println(" Data came in.. " + data);
//				client.sendEvent("chat message", data);
//			}
//		});
//		
//		this.server.addEventListener("pandasData", List.class, new DataListener<List>() {
//			@Override
//			public void onData(SocketIOClient client, List data, AckRequest ackRequest) {
//				// broadcast messages to all clients
//				System.out.println(client.getSessionId());
//				System.out.println(" Data came in.. " + data);
//				server.getBroadcastOperations().sendEvent("chat message", data);
//			}
//		});
//	}
//	
//	/**
//	 * Start the server
//	 */
//	public NettyLauncher startServer() {
//		server.start();
//		return this;
//	}
//	
//	/**
//	 * Stop the server
//	 */
//	public void stopServer() {
//		server.stop();
//	}
//
//	
//	
//	////////////////////////////////////////////////////////////
//	////////////////////////////////////////////////////////////
//	////////////////////////////////////////////////////////////
//	////////////////////////////////////////////////////////////
//	////////////////////////////////////////////////////////////
//
//	
//	/**
//	 * Main method to start the IO server
//	 * @param args
//	 * @throws InterruptedException
//	 */
//	public static void main(String[] args) throws InterruptedException {
//		NettyLauncher launcher = NettyLauncher.getInstance().startServer();
//		Thread.sleep(Integer.MAX_VALUE);
//	}
//
//
//}
