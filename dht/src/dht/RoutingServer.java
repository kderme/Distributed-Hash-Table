package dht;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;

//import org.apache.commons.codec.digest.DigestUtils;

public class RoutingServer extends Thread{
	protected String myIp,oneIp;
	protected int myPort,onePort;
	protected Server server=null;
	protected Console console;
	
	protected int myId;
	protected String myShaId;
	
	protected Socket socketOne, socketNext=null;
	protected PrintWriter outNext=null;

	  // A pre-allocated buffer
	protected final ByteBuffer buffer = ByteBuffer.allocate( 16384 );
	
	protected RoutingServer previous,next;
	protected String start,end;
	protected boolean amiFirst=false;
	
	//	fields for client
	protected Hashtable<String,SocketChannel> ht=new Hashtable<String,SocketChannel>();
	protected SocketChannel currentSC; //find iport of qeury here, not to change arguments of functions
	protected long socketId=0L;

	public RoutingServer(String myIp,int myPort,String oneIp,int onePort){
		this.myIp=myIp;
		this.myPort=myPort;
		this.oneIp=oneIp;
		this.onePort=onePort;
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
			outOne.println("HELLO-"+myIp+":"+myPort);

			//take first message from One
			String master =inOne.readLine();
			String [] spl=master.split("-");

			//---take id
			String mySid = spl[0];
			myId = Integer.parseInt(mySid);
			myShaId = hash(mySid);

			//----take range
			start=spl[1];
			end=spl[2];

			connectWithNext(spl[3]);

			takeAdditionalFromOne(spl);
			
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("One din`t respond! Exit");
			System.exit(1);
		}
		
		initServer();

		
		listen();
	}

	protected void initServer() {
		this.server=new Server(false,start,end);
	}

	protected void takeAdditionalFromOne(String[] spl) {}

	protected void listen(){
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
	                currentSC=sc;
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
		    ArrayList<String> ls=new ArrayList<String>();
		    while(true){
		    	//We assume messages that end with '\n' so we loop until newline
		    	//At every loop we add input in list ls and then we concat
			    	buffer.clear();
			    	int read=sc.read( buffer );
	console.log("position="+buffer.position()+" limit="+buffer.limit());
			    	buffer.flip();
	console.log("position="+buffer.position()+" limit="+buffer.limit());
		    	if (!sc.isConnected() || read==-1) {
				      return false;
		    	}
		    	if(read==0)
		    		continue;
		    	console.log("read="+read);
		    	byte [] bytes=new byte [read];
		    	buffer.get(bytes,0,read);
		    	
		    	String token=new String(bytes,java.nio.charset.StandardCharsets.UTF_8); //check encoding
		    	
		    	ls.add(token);
		    	byte b=bytes[bytes.length-1];
		    	Byte bb=new Byte(b);
		  
		    	if (bb.intValue()==10){
		    		break;
		    	}
		    	else{
		    		console.log("not newline at the end");
		    	}
		    }
		    // If no data, close the connection
		    
		    console.log("position="+buffer.position()+" limit="+buffer.limit());
		    
		    StringBuilder sb= new StringBuilder();

		    for(String tempString:ls){
		       sb.append(tempString);   
		     }
		    
		    String newMessage=sb.toString();
		    newMessage=newMessage.split(Character.valueOf((char)13).toString())[0];
		    processMessage(newMessage);
		    
		   // sc.write( buffer );
		    console.logExit();
		    return true;
		}


	protected void processMessage(String newMessage){
		System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!");
		console.logEntry();
		console.log("newMessage:" +newMessage);
		if(isItaBasicMessage(newMessage))
			;
		else if (isItAnotherMessage(newMessage))
			;
		else
			query(newMessage);
	}
	
	protected boolean isItaBasicMessage(String newMessage){
		if(newMessage.startsWith("LEAVE")){
			depart(newMessage);
		}
		
		else if (newMessage.startsWith("ANSWER-")){
			//	Answer<answer>
			System.out.println(newMessage.split("-")[1]);
			String a =newMessage.split("@",3)[1];
			
		}

		else if(newMessage.startsWith("NEWNEXT-")){
			//  NEWNEXT-ipNext:portNext
			connectWithNext(newMessage.split("-")[1]);
		}
		else if(newMessage.startsWith("BULK-")){
			String answer=server.action(newMessage);
			if(!answer.equals("OK"))
				depart("LeaveForced");
		}
		else
				return false;
		
		//code reaches here if newMessage type was found.
		//so stop searching and return
		return true;
	}
	
	protected boolean isItAnotherMessage(String newMessage){
		if(newMessage.startsWith("NEWLOW-")){
			//New Range due to new Node in network
			String[] spl=newMessage.split("-");
			updateStart(spl[1]);
			if(spl.length==3){
				// this means range become smaller (new came) so let`s send data at prev (=new)
				String reply=server.action("NEWLOW-"+start);
				sendMessage(spl[2],reply);
			}
		}
		else if(newMessage.startsWith("NEWDATA-")){
			//New Range due to new Node in network
			String[] spl=newMessage.split("-");
			updateStart(spl[1]);
			String reply=server.action(newMessage);
		}
		else if(newMessage.startsWith("NEWLOW2-")){
			//New Range due to new Node in network
			String[] spl=newMessage.split("-");
			if(spl.length==3){
				// this means range become smaller (new came) so let`s send data at prev (=new)
				String reply=server.action("NEWLOW2-"+spl[1]);
				sendMessage(spl[2],reply);
			}
		}
		else
			return false;
		
		return true;
	}
	
	protected void query(String newMessage){
		String sendMessage;
		if(!newMessage.startsWith("@")){
		/* 
		 * If this is the first hop
		 * add my #ip:port/socketId# to take Answer
		 */
			String[] parts=newMessage.split(",");
			if(!parts[0].equals("*")) parts[0]=hash(parts[0]);
			int i;
			newMessage=parts[0];
			for (i=1;i<parts.length;i++) newMessage=newMessage+","+parts[i];
			sendMessage="@"+myIp+":"+myPort+"/"+socketId+"@"+newMessage;
			ht.put(socketId+" ",currentSC);
			socketId++;
		}
		else{
			sendMessage=newMessage;
			//	else toss final ip
			newMessage=newMessage.split("@")[2];
		}
		String [] message=newMessage.split(",");
		String key=message[0];
		
		boolean isMine=isMine(key);
		if (isMine){
			String answer=server.action(newMessage);
		//	send back reply
			String [] sendBack=sendMessage.split("@")[1].split("/",2);
			sendMessage(sendBack[0], "ANSWER-"+sendBack[1]+"-"+answer);
		}
		else{
			outNext.println(sendMessage);
		}
	}
	
	/*
	 * iPort=<Ip>:<Port>
	 */	
	public void connectWithNext(String iPort){
		try {
		//---take routing info
		String next []=iPort.split(":");
		System.out.println("Told to connect to: "+next[0]+":"+next[1]);				
		//close existing connection (if exists)
		if(outNext!=null)
			outNext.close();
		if(socketNext!=null)
				socketNext.close();
		//open connection with next
		//**********************************
		if(iPort.equals(myIp+":"+myPort)) 
		{
			socketNext=null;
			outNext=null;
			return;
		}
		System.out.println("Port String has "+next[1].length()+" characters");
		//***********************************
		socketNext = new Socket(next[0],Integer.parseInt(next[1]));
		outNext = new PrintWriter(socketNext.getOutputStream(), true);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	protected void depart(String leaveMessage){
		console.logEntry();
		String message="Leaving-"+this.myShaId;
		boolean success=sendMessage(oneIp,onePort,message);
		if(!(success || leaveMessage.startsWith("LeaveForced"))){
			console.logExit();
			return;
		}
		//ready to leave
		String leave="Leaving";
		String myData=server.action(leave);
		this.outNext.println(myData);
		System.exit(0);
	}
	
	protected void updateStart(String start) {
		this.start=start;
		this.amiFirst=(compareHash(start,myShaId)>=0);		
	}

	protected boolean isMine(String key) {
		if(key.equals("*")) return true;
		if (compareHash(start,end)>=0)
			return compareHash(start,key)>0 || compareHash(key,end)>=0 ;
		return compareHash(start,key)<0  && compareHash(key,end)<=0 ;
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
		System.out.println("["+myIp+":"+myPort+"] Sending message: "+message);
		System.out.println("to: "+ip+":"+port);
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
		
	public static String hash(String s){
		return org.apache.commons.codec.digest.DigestUtils.sha1Hex(s);
	}
	
	public static String hash(int n){
		return hash(n+"");
	}
	
	public void main(String [] args){
		new RoutingServer("127.0.0.1", 5002,"127.0.0.1",5000);
		start();
	}
}
