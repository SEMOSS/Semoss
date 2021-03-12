package prerna.tcp.client;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldPrepender;

public class TCPChannelInitializer extends ChannelInitializer<SocketChannel> {

	Client nc = null;
	String inputSoFar = "";
	String endChar = "<o>";
	
	public void setClient(Client nc)
	{
		this.nc = nc;
	}
	
	@Override
	protected void initChannel(SocketChannel ch) throws Exception {
		// TODO Auto-generated method stub
        ChannelPipeline p = ch.pipeline();
        
        
        ClientHandler nch2 = new ClientHandler();
        nch2.setClient(nc);
        //p.addLast(new LoggingHandler(LogLevel.INFO));
        p.addLast(new LengthFieldPrepender(4));
        p.addLast(nch2);
		
	}

}
