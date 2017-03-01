package dht;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.commons.codec.digest.DigestUtils;

public class RoutingServer {
	private String myIp,oneIp;
	private int myPort,onePort;
	private int k; //copies, default is 1, which means no copies
	private Server Server;
	
	private int myId;
	private String myShaId;
	
	private Socket socketOne, socketNext=null;
	ServerSocket socketPrev;
	private PrintWriter outOne, outNext=null;
	private BufferedReader inOne;

	RoutingServer previous,next;
	private long start,end;	
	
	private RoutingServer(String myIp,int myPort,int k,String oneIp,int onePort){
		this.myIp=myIp;
		this.myPort=myPort;
		this.k=k;
		this.oneIp=oneIp;
		this.onePort=onePort;
		this.Server=new Server(false,"",""); 
	}
	
	/*
	 * 		Me:		Hello-<myIp>:<myPort>
	 * 		One:	<yourId>-<ipNext>:<portNext>-start-end
	 */
	private void start (){
		
		try {
			socketOne = new Socket(oneIp, onePort);
			outOne= new PrintWriter(socketOne.getOutputStream(), true);
			inOne = new BufferedReader(
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
			long start=new Long(spl[1]);
			long end=new Long(spl[2]);
			
			connectWithNext(spl[3]);

		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("One din`t respond! Exit");
			System.exit(1);
		}
		
		listen();
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
	
	public void sendMessage(String iPort,String message){
	try{	
		//---take routing info
		String next []=iPort.split(":");
		
		Socket socket = new Socket(next[0],new Integer(next[1]));
		PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
		out.println(message);
		socket.close();
	} catch(IOException e){
		e.printStackTrace();
	}
	}
	
	private void listen(){
		try{
		socketPrev = new ServerSocket(myPort);
		System.out.println("Listening");
		while(true){
			Socket sock=socketPrev.accept();
			System.out.println("Connected");
			BufferedReader inPrev = new BufferedReader(
	                new InputStreamReader(sock.getInputStream()));
			String newMessage=inPrev.readLine();
			System.out.println(newMessage);
			
			
			if (newMessage.startsWith("Answer-")){
				//if an answer, comes just print it (?)
				//	Answer-<answer>
				System.out.println(newMessage.split("-")[1]);
				continue;
			}
	
			if(newMessage.startsWith("One-")){
				//	Some change happened (someone died)
				//  One-ipNext:portNext
				connectWithNext(newMessage.split("-")[1]);
				continue;
			}
			
			if(newMessage.startsWith("OneLeft-")){
				//  TODO : One Died ?
			}

			String sendMessage;
			if(!newMessage.contains("#")){
				/* 
				 * If this is the first hop
				 * add my ip:port# to take Answer
				 */
				sendMessage=myIp+":"+myPort+"#"+newMessage;
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
	}
	catch(IOException e){
		e.printStackTrace();
	}
	}
	
	private String reform(String[] message) {
		/*
		 * TODO read how many copies already done 
		 * and reduce the amount of copies left
		 */
		return message.toString();
	}

	private boolean isMine(String key) {
		Long sha1=new Long(hash(key));
		return (sha1>start && sha1<end);  
	}

	private String getKey(String message) {
		return null;
	}
	
	private String hash(String s){
		return org.apache.commons.codec.digest.DigestUtils.sha1Hex("");
	}
	
	private String hash(int n){
		return hash(n+"");
	}
	
	public void main(String [] args){
		
		new RoutingServer("127.0.0.1", 5002, 1,"127.0.0.1",5000);
		start();
	}
}
