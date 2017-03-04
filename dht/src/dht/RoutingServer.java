package dht;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.codec.digest.DigestUtils;

public class RoutingServer extends Thread{
	protected String myIp,oneIp;
	protected int myPort,onePort;
	private int k; //copies, default is 1, which means no copies
	private Server Server;
	protected Console console;
	
	protected int myId;
	protected String myShaId;
	
	private Socket socketOne, socketNext=null;
	private PrintWriter outNext=null;

	  // A pre-allocated buffer for encrypting data
	  private final ByteBuffer buffer = ByteBuffer.allocate( 16384 );
	
	RoutingServer previous,next;
	protected String start,end;
	protected boolean amiFirst=false;

	protected RoutingServer(String myIp,int myPort,int k,String oneIp,int onePort){
		this.myIp=myIp;
		this.myPort=myPort;
		this.k=k;
		this.oneIp=oneIp;
		this.onePort=onePort;
		this.Server=new Server(false,"",""); 
		this.console=new Console();
	}

	/*
	 * 		Me:		Hello-<myIp>:<myPort>
	 * 		One:	<yourId>-<start>-<end>-<ipNext>:<portNext>
	 */
	public void run(){
		console.logEntry();
		try {
			socketOne = new Socket(oneIp, onePort);
			PrintWriter outOne= new PrintWriter(socketOne.getOutputStream(), true);
			BufferedReader inOne = new BufferedReader(
	                new InputStreamReader(socketOne.getInputStream()));

			//inform One
			outOne.println("Hello-"+myIp+":"+myPort);

			//take first message from One
			String master =inOne.readLine();
			String [] spl=master.split("-");

			//---take id
			String mySid = spl[0];
			myId = new Integer(mySid);
			myShaId = hash(mySid);

			//----take range
			start=spl[1];
			end=spl[2];
			
			connectWithNext(spl[3]);
			
			

		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("One din`t respond! Exit");
			System.exit(1);
		}

		listen();
	}

	private void listen(){
		console.logEntry();
	    try {
	        // Instead of creating a ServerSocket,
	        // create a ServerSocketChannel
	        ServerSocketChannel ssc = ServerSocketChannel.open();

	        // Set it to non-blocking, so we can use select
	        ssc.configureBlocking( false );

	        // Get the Socket connected to this channel, and bind it
	        // to the listening port
	        ServerSocket ss = ssc.socket();
	        InetSocketAddress isa = new InetSocketAddress( myPort );
	        ss.bind( isa );

	        // Create a new Selector for selecting
	        Selector selector = Selector.open();

	        // Register the ServerSocketChannel, so we can
	        // listen for incoming connections
	        ssc.register( selector, SelectionKey.OP_ACCEPT );
	        System.out.println( "Listening on port "+myPort );

	        while (true) {
	          // See if we've had any activity -- either
	          // an incoming connection, or incoming data on an
	          // existing connection
	          int num = selector.select();

	          // If we don't have any activity, loop around and wait
	          // again
	          if (num == 0) {
	            console.log("num==0");
	            continue;
	          }

	          // Get the keys corresponding to the activity
	          // that has been detected, and process them
	          // one by one
	          Set keys = selector.selectedKeys();
	          Iterator it = keys.iterator();
	          while (it.hasNext()) {
	            // Get a key representing one of bits of I/O
	            // activity
	            SelectionKey key = (SelectionKey)it.next();

	            // What kind of activity is it?
	            if ((key.readyOps() & SelectionKey.OP_ACCEPT) ==
	              SelectionKey.OP_ACCEPT) {

	            	console.log( "New connection" );
	              // It's an incoming connection.
	              // Register this socket with the Selector
	              // so we can listen for input on it

	              Socket s = ss.accept();
	              System.out.println( "Got connection from "+s );

	              // Make sure to make it non-blocking, so we can
	              // use a selector on it.
	              SocketChannel sc = s.getChannel();
	              sc.configureBlocking( false );

	              // Register it with the selector, for reading
	              sc.register( selector, SelectionKey.OP_READ );
	            } else if ((key.readyOps() & SelectionKey.OP_READ) ==
	              SelectionKey.OP_READ) {

	              console.log("New Data");
	              SocketChannel sc = null;

	              try {

	                // It's incoming data on a connection, so
	                // process it
	                sc = (SocketChannel)key.channel();
	                boolean ok = processInput( sc );

	                // If the connection is dead, then remove it
	                // from the selector and close it
	                if (!ok) {
	                  key.cancel();

	                  Socket s = null;
	                  try {
	                    s = sc.socket();
	                    s.close();
	                  } catch( IOException ie ) {
	                    console.log( "Error closing socket "+s+": "+ie );
	                  }
	                }

	              } catch( IOException ie ) {

	                // On exception, remove this channel from the selector
	                key.cancel();
	                //TODO also close it if it`s not One or Prev. Find this out from processInput
	                try {
	                  sc.close();
	                } catch( IOException ie2 ) { console.log( ie2 ); }

	                console.log( "Closed "+sc );
	              }
	            }
	          }

	          // We remove the selected keys, because we've dealt
	          // with them.
	          keys.clear();
	        }
	      } catch( IOException ie ) {
	        System.err.println( ie );
	      }
	    }

  private boolean processInput( SocketChannel sc ) throws IOException {
	    console.logEntry();
	    buffer.clear();
	    sc.read( buffer );
	    buffer.flip();

	    // If no data, close the connection
	    if (buffer.limit()==0) {
	      return false;
	    }
	    byte[] bytes=buffer.array();
	    
	    String newMessage=new String(bytes,java.nio.charset.StandardCharsets.UTF_8); //check encoding
	    processMessage(newMessage);
	    
	   // sc.write( buffer );
	    console.logExit();
	    return true;
	  }

	protected void processMessage(String newMessage){
		console.logEntry();
		console.log("newMessage:" +newMessage);
		if(newMessage.startsWith("Leave")){
			depart(newMessage);
			// we may not manage to leave
			return;
		}
		
		if (newMessage.startsWith("ANSWER-")){
			//if an answer, comes just print it (?)
			//	Answer-<answer>
			System.out.println(newMessage.split("-")[1]);
			return;
		}

		if(newMessage.startsWith("NEWNEXT-")){
			//	Some change happened (someone left or came)
			//  One-ipNext:portNext
			try{
				connectWithNext(newMessage.split("-")[1]);
			}
			catch(IOException e){
				e.printStackTrace();
				System.out.println("error while connecting with next");
				depart("LeaveForced");
			}
			return;
		}

		if(newMessage.startsWith("NEWLOW-")){
			//New Range due to new Node in network
			String[] spl=newMessage.split("-");
			start=spl[1];
			amiFirst=(compareHash(start,myShaId)>=0);
			if(spl.length==3){
				// this means range become smaller (new came) so let`s send data at prev (=new)
				String reply=Server.action("NewRange-"+start);
				sendMessage(spl[2],reply);
			}
			return;
		}

		if(newMessage.startsWith("BULK-")){
//			String data=newMessage.split("-",2)[1];
			String answer=Server.action(newMessage);
			if(!answer.equals("OK"))
				depart("LeaveForced");
			return;
		}

		if(newMessage.startsWith("ONELEFT-")){
			//  TODO : One Died ?
			return;
		}

		String sendMessage;
		if(!newMessage.startsWith("#")){
			/* 
			 * If this is the first hop
			 * add my #ip:port# to take Answer
			 */
			sendMessage="#"+myIp+":"+myPort+"#"+newMessage;
		}
		else{
			//	else toss final ip
			sendMessage=newMessage;
			newMessage=newMessage.split("#")[1];
		}
		String [] message=newMessage.split(",");
		String key=message[0];

		boolean isMine=isMine(key);
		if (isMine){
			String answer=Server.action(newMessage);
			//send back reply
			sendMessage(sendMessage.split("#")[0], answer);
		}
		else{
			
			outNext.println(sendMessage);
		}

	}
	
	/*
	 * This function is called at start or when 
	 * a routing change happens (someone left or came)
	 * iPort=<Ip>:<Port>
	 */
	
	public void connectWithNext(String iPort) throws IOException{

		//---take routing info
		String next []=iPort.split(":");
							
		//close existing connection (if exists)
		if(outNext!=null)
			outNext.close();
		if(socketNext!=null)
			socketNext.close();
		
		//open connection with next
		socketNext = new Socket(next[0],new Integer(next[1]));
		outNext = new PrintWriter(socketNext.getOutputStream(), true);
	}

	public void depart(String leaveMessage){
		console.logEntry();
		String message="Leaving-"+this.myShaId;
		boolean success=sendMessage(oneIp,onePort,message);
		if(!(success || leaveMessage.startsWith("LeaveForced"))){
			console.logExit();
			return;
		}
		//ready to leave
		String leave="Leaving";
		String myData=Server.action(leave);
		this.outNext.println(myData);
		System.exit(0);
	}

	private String reform(String[] message) {
		/*
		 * TODO read how many copies already done 
		 * and reduce the amount of copies left
		 */
		return message.toString();
	}

	protected boolean isMine(String key) {
		if (amiFirst)
			return compareHash(start,key)<0 || compareHash(key,end)<0 ;
		return compareHash(start,key)<0  && compareHash(key,end)<0 ;
	}

	private String getZeroSha1() {
		// TODO Auto-generated method stub
		String zero="0000000000";	//10 
		return zero+zero+zero+zero;
	}

	private String getMaxSha1() {
		// TODO find decoding used for 2**160-1
		return null;
	}

	public static int compareHash(String h1,String h2){
		for(int i=0;i<40;i++){
			int d=h1.charAt(i)-h2.charAt(i);
			if(d!=0)
			return d;
		}
		return 0;
	}

	protected boolean sendMessage(String ip, int port, String message){
		try{
			Socket socket = new Socket(ip,port);
			PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
			out.println(message);
			socket.close();
			return true;
		} catch(IOException e){
			e.printStackTrace();
			return false;
		}
	}

	protected boolean sendMessage(String iPort,String message){
		
		//---take routing info
		String next []=iPort.split(":");
		
		return sendMessage(next[0],new Integer(next[1]),message);
	}

	private void listen_old(){
		try{
			ServerSocket srvSocket= new ServerSocket(myPort,7); //backlog (max queue)
		System.out.println("Listening");
		while(true){
			Socket sock=srvSocket.accept();
			System.out.println("Connected");
			BufferedReader inPrev = new BufferedReader(
	                new InputStreamReader(sock.getInputStream()));
			String newMessage=inPrev.readLine();
			System.out.println(newMessage);
		
			processMessage(newMessage);
					}
	}	
	catch(IOException e){
		e.printStackTrace();
	}
	}
	
	private String getKey(String message) {
		return null;
	}
	
	public static String hash(String s){
		return org.apache.commons.codec.digest.DigestUtils.sha1Hex("");
	}
	
	public static String hash(int n){
		return hash(n+"");
	}
	
	public void main(String [] args){
		new RoutingServer("127.0.0.1", 5002, 1,"127.0.0.1",5000);
		start();
	}
}
