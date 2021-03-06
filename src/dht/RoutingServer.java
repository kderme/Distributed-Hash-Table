package dht;
import java.io.BufferedReader;
import java.io.File;
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
	protected int leave_count=0;
	protected int myId;
	protected String myShaId;
	protected boolean check_interrupted=false;
	protected Socket socketOne, socketNext=null;
	protected PrintWriter outNext=null;

	  // A pre-allocated buffer
	protected final ByteBuffer buffer = ByteBuffer.allocate( 16384 );
	
	protected RoutingServer previous,next;
	protected String start,end;
	protected boolean amiFirst=false;
	
	//	fields for client
	protected Hashtable<Integer,SocketChannel> ht=new Hashtable<Integer,SocketChannel>();
	protected SocketChannel currentSC; //find sc here, not to change arguments of functions
	protected Integer currentClid=new Integer(0);
	
	protected Hashtable<Integer,ArrayList<String>> data=new Hashtable<Integer,ArrayList<String>>();
	protected Hashtable<Integer,Integer> numberOfNodes=new Hashtable<Integer,Integer>();

	public RoutingServer(String myIp,int myPort,String oneIp,int onePort){
		this.myIp=myIp;
		this.myPort=myPort;
		this.oneIp=oneIp;
		this.onePort=onePort;
		this.console=new Console(myPort+"","logs"+File.separator+myPort+".txt");
		console.log("Routing Server");
		//this.console=new Console("D:\\output"+myPort+".txt");
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
			//TODO we keep in track number of nodes. Id is not the best way
			myShaId = Utilities.hash(mySid);

			//----take range
			start=spl[1];
			end=spl[2];

			connectWithNext(spl[3]);

			takeAdditionalFromOne(spl);
			
		} catch (IOException e) {
			e.printStackTrace();
			console.log("One din`t respond! Exit");
			System.exit(1);
		}
		
		initServer();

		
		listen();
	}

	protected void initServer() {
		this.server=new Server(false,start,end,console);
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
	        console.log("Listening on port "+myPort );
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
	          boolean further=further_checking();
	          if(further && check_interrupted && !keys.iterator().hasNext()) break;
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
	              console.log("Got connection from "+s );

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
	                } catch( IOException ie2 ) { console.log(ie2 ); }

	                console.log( "Closed "+sc );
	              }
	            }
	          }

	          // We remove the selected keys, because we've dealt
	          // with them.
	          keys.clear();
	        }
	      } catch( IOException ie ) {
	    	  System.out.println("Exception happened");
	        System.err.println( ie );
	      }
	    System.out.println("There "+leave_count);
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
		    String[] Messages;
		    if(!System.getProperty("os.name").equalsIgnoreCase("Linux")) Messages=newMessage.split(Character.valueOf((char)13).toString()+Character.valueOf((char)10).toString());
		    else Messages=newMessage.split(Character.valueOf((char)10).toString());
		    int index=0;
		    //System.out.println(Messages.length);
		    while(index<Messages.length)
		    {
		    	//System.out.println(Messages[index]);
		    	if(!(Messages[index].equals(Character.valueOf((char)10).toString()) || Messages[index].equals(Character.valueOf((char)13).toString())))
		    		{
		    			//System.out.println("YES");
		    			processMessage(Messages[index]);
		    		}
		    	index++;
		    }
		    
		   // sc.write( buffer );
		    console.logExit();
		    return true;
		}


	protected void processMessage(String newMessage){
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
		else if(newMessage.startsWith("PING")){
			console.log("GOT PINGED");
		}
		else if(newMessage.startsWith("PINGNEXT")){
			outNext.println("PING");
		}
		else if(newMessage.startsWith("TRACEROUTE")){
			String [] spl=newMessage.split("-",2);
			int ttl=Integer.parseInt(spl[1]);
			if(ttl>0)
				outNext.println("TRACEROUTE-"+(ttl-1));
		}
		else if (newMessage.startsWith("ANSWER")){
			//	Answer<answer>
			String [] spl=newMessage.split("-",3);
			Integer currentClid =Integer.valueOf(spl[1]);
			SocketChannel sc=ht.get(currentClid);
			if (newMessage.startsWith("ANSWER*"))
				answerStar(currentClid,spl[2],sc);
			else{
				console.log("For user id: "+currentClid+" the answer is: "+spl[2]);
				sendClient(sc,spl[2]);
			}
		}

		else if(newMessage.startsWith("NEWNEXT-")){
			//  NEWNEXT-ipNext:portNext
			connectWithNext(newMessage.split("-")[1]);
		}
		else if(newMessage.startsWith("BULK-")){
			String answer;
			if(newMessage.split("-").length>1) answer=server.action(newMessage);
			else answer="OK";
			if(!answer.equals("OK"))
				{
					console.log("OOPS, answer is: "+answer);
					depart("LeaveForced");
				}
		}
		else
				return false;
		
		//code reaches here if newMessage type was found.
		//so stop searching and return
		return true;
	}
	
	private void answerStar(Integer ClientId, String message, SocketChannel sc) {
		data.get(ClientId).add(message);
		if(data.get(ClientId).size()==this.numberOfNodes.get(Integer.valueOf(ClientId)) && this.numberOfNodes.get(Integer.valueOf(ClientId))!=0){
			//if all nodes (even me) have replied, append all replies and send to client
			StringBuilder sb= new StringBuilder();

		    for(String tempString:data.get(ClientId)){
		    	if(tempString!=null && !tempString.equals("null") && tempString.length()>0){
		    		sb.append(tempString);  
		    		sb.append("_");
		    	}
		     }
		    
		    String reply=sb.toString();
		    if(reply!=null && !reply.equals("null") && reply.length()>0) reply=reply.substring(0,reply.length()-1);
		    console.log("For user id: "+ClientId+" the answer is: "+reply);
		    sendClient(sc,reply);
		}
		// TODO Auto-generated method stub
		
	}

	private void sendClient(SocketChannel sc, String string) {
		console.logEntry();
		console.log("reply for client:"+string);
		string+="\n";
		byte [] bs=string.getBytes();
		ByteBuffer sendBack = ByteBuffer.wrap(bs);
console.log("position="+sendBack.position()+" limit="+sendBack.limit());

	try {
		int write=-1,  counter=0;
		int totalWrite=0;
		while(totalWrite<sendBack.limit()){
			counter++;
console.log("position="+sendBack.position()+" limit="+sendBack.limit());
			write=sc.write(sendBack);
console.log("position="+sendBack.position()+" limit="+sendBack.limit());
			totalWrite+=write;
			console.log("write="+write+". Total write="+totalWrite);
			
			if(totalWrite==0 && counter>30){
				//sc.register( selector, SelectionKey.OP_WRITE );
				console.log("Write Failed multiple times");
				return;
			}
		}
	} catch (IOException e) {		
		e.printStackTrace();
	}
	console.logExit();
	}

	protected boolean isItAnotherMessage(String newMessage){
		if(newMessage.startsWith("NEWLOW-")){
			//New Range due to new Node in network
			String[] spl=newMessage.split("-");
			updateStart(spl[1]);
			if(spl.length==3){
				// this means range become smaller (new came) so let`s send data at prev (=new)
				String reply=server.action("NEWLOW-"+start);
				Utilities.sendMessage(spl[2],reply,console);
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
			leave_count++;
			String[] spl=newMessage.split("-");
			if(spl.length==3){
				// this means range become smaller (new came) so let`s send data at prev (=new)
				String reply=server.action("NEWLOW2-"+spl[1]);
				Utilities.sendMessage(spl[2],reply,console);
			}
		}
		else
			return false;
		
		return true;
	}
	
	protected void query(String newMessage){
		console.logEntry();
		console.log("low="+start+" high="+end);
		String sendMessage;
		if(!newMessage.startsWith("@")){
		/* 
		 * If this is the first hop
		 * add my #ip:port/currentClid# to take Answer
		 */
			String[] parts=newMessage.split(",",2);
			if(!parts[0].equals("*")) parts[0]=Utilities.hash(parts[0]);
			int i;
			newMessage=parts[0];
			for (i=1;i<parts.length;i++) newMessage=newMessage+","+parts[i];
			sendMessage="@"+myIp+":"+myPort+"/"+currentClid+"@"+newMessage;
			ht.put(currentClid,currentSC);
			
			if(parts[0].equals("*"))
				data.put(currentClid,new ArrayList<String>());

			currentClid++;
		}
		else{
			sendMessage=newMessage;
			//	else toss final ip
			newMessage=newMessage.split("@")[2];
		}
		String [] message=newMessage.split(",");
		String key=message[0];
		console.log("Key to be tested is: "+key);
		boolean isMine=isMine(key);
		if (isMine){
			console.log("my message:"+newMessage);
			if(key.equals("*"))
			{
				if(message.length==2)
				{
					String [] sendBack=sendMessage.split("@")[1].split("/",2);
					sendMessage=sendMessage+",1";
					this.numberOfNodes.put(Integer.valueOf(sendBack[1]),new Integer(0));
					console.log("Sending to next node: "+sendMessage);
					if(outNext!=null )outNext.println(sendMessage);
					else
					{
						String answer=server.action(newMessage);
						String token="ANSWER*";
						this.numberOfNodes.put(Integer.valueOf(sendBack[1]),new Integer(1));
						Utilities.sendMessage(sendBack[0], token+"-"+sendBack[1]+"-"+answer,console);
					}
				}
				else
				{
					String answer=server.action(newMessage);
					//	send back reply
					String [] sendBack=sendMessage.split("@")[1].split("/",2);
					String token="ANSWER*";
					sendMessage="@"+sendBack[0]+"/"+sendBack[1]+"@"+message[0]+","+message[1]+","+(Integer.parseInt(message[2])+1);
					if(key.equals("*") && !sendBack[0].equals(myIp+":"+myPort)) {
						console.log("Sending to next node: "+sendMessage);
						outNext.println(sendMessage);
					}
					else this.numberOfNodes.put(Integer.valueOf(sendBack[1]),Integer.parseInt(message[2]));
	Utilities.sendMessage(sendBack[0], token+"-"+sendBack[1]+"-"+answer,console);
				}	
			}
			else
			{
				String answer=server.action(newMessage);
				//	send back reply
				String [] sendBack=sendMessage.split("@")[1].split("/",2);
				String token="ANSWER";
	Utilities.sendMessage(sendBack[0], token+"-"+sendBack[1]+"-"+answer,console);
			}
		}
		else{
			console.log("Not my message:"+newMessage);
			outNext.println(sendMessage);
		}
		console.logExit();
	}
	
	/*
	 * iPort=<Ip>:<Port>
	 */	
	public void connectWithNext(String iPort){
		
		//---take routing info
		String next []=iPort.split(":");
		console.log("Told to connect to: "+next[0]+":"+next[1]);				
		//close existing connection (if exists)
		if(outNext!=null)
		{
			outNext.close();
			outNext=null;
		}
		if(socketNext!=null)
			try{
			socketNext.close();
			socketNext=null;
			} catch (IOException e1) {
                console.log("");
                // TODO Auto-generated catch block
                e1.printStackTrace();
				}
		//open connection with next
		//**********************************
		if(iPort.equals(myIp+":"+myPort)) 
		{
			socketNext=null;
			outNext=null;
			return;
		}
		console.log("Port String has "+next[1].length()+" characters");
		//***********************************
		console.log(next[0]);
        console.log(next[1]);
        console.log("I`m not leaving until I connect!");
        while(true){
                try {
                socketNext = new Socket(next[0],Integer.parseInt(next[1]));
                outNext = new PrintWriter(socketNext.getOutputStream(), true);

                } catch (IOException e) {
                        console.log("nop");
                        continue;
                }
                break;
        }
	}
	protected void depart(String leaveMessage){
		leave_count=0;
		console.logEntry();
		String message="Leaving-"+this.myShaId;
		if(!leaveMessage.startsWith("LeaveForced") && 
				Utilities.sendMessageWithReply(oneIp,onePort,message,console).equals("Removed")){
			console.logExit();
			check_interrupted=true;
			return;
		}
		//ready to leave
		String leave="LEAVE-"+myShaId;
		String myData=server.action(leave);
		console.log("Sending to next node: "+myData);
		this.outNext.println(myData);
		check_interrupted=true;
	}
	protected boolean further_checking(){return true;}
	
	protected void updateStart(String start) {
		this.start=start;
		this.amiFirst=(Utilities.compareHash(start,myShaId)>=0);		
	}

	protected boolean isMine(String key) {
		if(key.equals("*")) return true;
		if (Utilities.compareHash(start,end)>=0)
			return Utilities.compareHash(start,key)<=0 || Utilities.compareHash(key,end)<0 ;
		return Utilities.compareHash(start,key)<=0  && Utilities.compareHash(key,end)<0 ;
	}

	
	public void main(String [] args){
		new RoutingServer("127.0.0.1", 5002,"127.0.0.1",5000);
		start();
	}
}
