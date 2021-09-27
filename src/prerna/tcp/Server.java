package prerna.tcp;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.ConfigurationSource;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.oio.OioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.oio.OioServerSocketChannel;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import jep.Jep;
import prerna.ds.py.PyExecutorThread;
import prerna.sablecc2.reactor.frame.r.util.RJavaJriTranslator;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Settings;
import prerna.util.Utility;

public class Server 
{
	
	// basically a process which works by looking for commands in TCP space
	private static final String CLASS_NAME = Server.class.getName();

	List <String> commandList = new ArrayList<String>(); // this is giving the file name and that too relative
	public  static Logger LOGGER = null;

	String internalLock = "Internal Lock";
	private static boolean first = true;
	public Jep jep = null;
	boolean SSL = false;
	
	public String [] command = null;
	public Hashtable <String, Object> response = new Hashtable<String, Object>();
	
	public volatile boolean keepAlive = true;
	public volatile boolean ready = false;
	public Object driverMonitor = null;
	
	Properties prop = null; // this is basically reference to the RDF Map
	List foldersBeingWatched = new ArrayList();
	public String mainFolder = null;
	PyExecutorThread pt = null;
	RJavaJriTranslator rt = null;

	static boolean test = false;

    EventLoopGroup bossGroup = null;
    EventLoopGroup workerGroup = null;

	
	public static void main(String [] args)
	{
		
		
		// arg1 - the directory where commands would be thrown
		// arg2 - access to the rdf map to load
		// arg3 - port to start
		
		// create the watch service
		// start this thread
		
		// when event comes write it to the command
		// comment this for main execution
		//-Dlog4j.defaultInitOverride=TRUE
		
		if(args == null || args.length == 0)
		{
			args = new String[4];
			args[0] = "C:\\users\\pkapaleeswaran\\workspacej3\\SemossDev\\config";
			args[1] = "C:\\users\\pkapaleeswaran\\workspacej3\\SemossDev\\RDF_Map.prop";;
			args[2] = "9999";
			//args[3] = "py";
			args[3] = "r";
			test = true;
				
		}
		
		String log4JPropFile = Paths.get(Utility.normalizePath(args[0]), "log4j2.properties").toAbsolutePath().toString();
		
		FileInputStream fis = null;
		ConfigurationSource source = null;
		try {
			fis = new FileInputStream(Utility.normalizePath(log4JPropFile));
			source = new ConfigurationSource(fis);
			//Configuration con = PropertiesConfigurationFactory.getInstance().getConfiguration(new LoggerContext(CLASS_NAME), source);
			//LOGGER = con.getLoggerContext().getLogger(CLASS_NAME);
			//LOGGER = Configurator.initialize(null, source).getLogger(CLASS_NAME);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		
		Server worker = new Server();
		DIHelper.getInstance().loadCoreProp(args[1]);
		DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		
		worker.prop = new Properties();
		
		try(FileInputStream fileInput = new FileInputStream(Utility.normalizePath(args[1]))) {
			worker.prop.load(fileInput);
			System.out.println("Loaded the rdf map");
			
			// get the library for jep
			//String jepLib = worker.prop.getProperty("JEP_LIB");
			
			//System.loadLibrary(jepLib);
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		worker.mainFolder = args[0];
		String engine = "r"; // setting it up for r
		if(args.length == 4)
			engine = args[3];
		worker.bootServer(Integer.parseInt(args[2]), engine);
	}
	
	public void startPyExecutor()
	{
		if(this.pt== null)
		{
			pt = new PyExecutorThread();
			//pt.getJep();
			pt.start();
			
			
			while(!pt.isReady())
			{
				try {
					// sleep until we get the py
					Thread.sleep(200);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			
			LOGGER.info("PyThread Started");
		}
	}
	
	
	public void bootServer(int PORT, String engine)
	{
        // Configure SSL.
        // Configure the server.
		LOGGER = LogManager.getLogger(CLASS_NAME);
        
		boolean blocking = DIHelper.getInstance().getProperty(Settings.BLOCKING) != null && DIHelper.getInstance().getProperty(Settings.BLOCKING).equalsIgnoreCase("true");
        
        if(blocking)
        {
        	bossGroup = new OioEventLoopGroup();
        	workerGroup = new OioEventLoopGroup();
        }
        else
        {
    		bossGroup = new NioEventLoopGroup();
            workerGroup = new NioEventLoopGroup(20);
        }
        
        try {
            final SslContext sslCtx;
            if (SSL) {
                SelfSignedCertificate ssc = new SelfSignedCertificate();
                sslCtx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
            } else {
                sslCtx = null;
            }

            ServerBootstrap b = new ServerBootstrap();
            if(blocking)
            {
	            b.group(bossGroup, workerGroup)
	             //.channel(NioServerSocketChannel.class)
	             .channel(OioServerSocketChannel.class)
	             .option(ChannelOption.SO_BACKLOG, 100)
	             .option(ChannelOption.TCP_NODELAY, true)
	             .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(800*1024, 1024*1024))
	
	            // .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(256*1024, 512*1024))
	//             .option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, (1024*1024))
	//             .option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, (512*1024))
	             .handler(new LoggingHandler(LogLevel.INFO))
	             .childHandler(new ChannelInitializer<SocketChannel>() {
	                 @Override
	                 public void initChannel(SocketChannel ch) throws Exception {
	                     ChannelPipeline p = ch.pipeline();
	                     if (sslCtx != null) {
	                         p.addLast(sslCtx.newHandler(ch.alloc()));
	                     }
	                	 // start the R engine
	                     //TCPRServerHandler tsh = new TCPRServerHandler();
	                     BinaryServerHandler tsh = new BinaryServerHandler();
	                     tsh.setBossGroup(bossGroup);
	                     tsh.setWorkerGroup(workerGroup);
	                     tsh.setLogger(LOGGER);
	                     //tsh.setRJavaTranslator(rt);
	                     tsh.setPyExecutorThread(pt);
	                     tsh.setMainFolder(mainFolder);
	                     tsh.setTest(test);
	                     p
	                     .addLast(new LengthFieldPrepender(4))
	                     .addLast(tsh);
	                 }
	             });
            }
            else
            {
	            b.group(bossGroup, workerGroup)
	             .channel(NioServerSocketChannel.class)
	             .option(ChannelOption.SO_BACKLOG, 100)
	             .option(ChannelOption.TCP_NODELAY, true)
	             .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(800*1024, 1024*1024))
	
	            // .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(256*1024, 512*1024))
	//             .option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, (1024*1024))
	//             .option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, (512*1024))
	             .handler(new LoggingHandler(LogLevel.INFO))
	             .childHandler(new ChannelInitializer<SocketChannel>() {
	                 @Override
	                 public void initChannel(SocketChannel ch) throws Exception {
	                     ChannelPipeline p = ch.pipeline();
	                     if (sslCtx != null) {
	                         p.addLast(sslCtx.newHandler(ch.alloc()));
	                     }
	                	 // start the R engine
	                     //TCPRServerHandler tsh = new TCPRServerHandler();
	                     BinaryServerHandler tsh = new BinaryServerHandler();
	                     tsh.setBossGroup(bossGroup);
	                     tsh.setWorkerGroup(workerGroup);
	                     tsh.setLogger(LOGGER);
	                     //tsh.setRJavaTranslator(rt);
	                     tsh.setPyExecutorThread(pt);
	                     tsh.setMainFolder(mainFolder);
	                     tsh.setTest(test);
	                     p
	                     .addLast(new LengthFieldPrepender(4))
	                     .addLast(tsh);
	                 }
	             });
            	
            }
            
            // create the engines
        	startPyExecutor();
            
            // Start the server.
            ChannelFuture f = b.bind(PORT).sync();
            LOGGER.info("Listening on port " + PORT);
            LOGGER.info("set watermarks");
            // Wait until the server socket is closed.
            f.channel().closeFuture().sync();
        }catch (IOException ex)
        {
        	LOGGER.debug("Connection Closed ");
        }catch(Exception ex)
        {
        	LOGGER.debug(ex);
        }
        finally 
        {
            // Shut down all event loops to terminate all threads.
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
		
	}

}
