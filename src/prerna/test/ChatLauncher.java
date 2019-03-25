//package prerna.test;
//
//import com.corundumstudio.socketio.AckRequest;
//import com.corundumstudio.socketio.Configuration;
//import com.corundumstudio.socketio.SocketIOClient;
//import com.corundumstudio.socketio.SocketIOServer;
//import com.corundumstudio.socketio.listener.ConnectListener;
//import com.corundumstudio.socketio.listener.DataListener;
//
//public class ChatLauncher {
//
//    public static void main(String[] args) throws InterruptedException {
//
//        Configuration config = new Configuration();
//        config.setHostname("localhost");
//        config.setPort(3000);
//
//        final SocketIOServer server = new SocketIOServer(config);
//        
//        server.addConnectListener(new ConnectListener(){
//
//			@Override
//			public void onConnect(SocketIOClient client) {
//				// TODO Auto-generated method stub
//			
//				System.out.println("Client Connected.. " + client.getSessionId());
//                client.sendEvent("chat message", "welcome from NETTY");
//			}
//        	
//        });
//        
//        server.addEventListener("chat message", String.class, new DataListener<String>() {
//            @Override
//            public void onData(SocketIOClient client, String data, AckRequest ackRequest) {
//                // broadcast messages to all clients
//            	System.out.println(client.getSessionId());
//            	System.out.println(" Data came in.. " + data);
//                server.getBroadcastOperations().sendEvent("chat message", data);
//            }
//        });
//
//        server.start();
//
//        Thread.sleep(Integer.MAX_VALUE);
//
//        server.stop();
//    }
//
//}