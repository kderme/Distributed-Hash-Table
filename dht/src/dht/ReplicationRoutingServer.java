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

public class ReplicationRoutingServer extends RoutingServer{
	protected int k=1;
	protected String startReplica;
	protected boolean amiCut;
	private int consistency=0;

	public ReplicationRoutingServer(String myIp,int myPort,String oneIp,int onePort,int k,int consistency){
		super(myIp,myPort,oneIp,onePort);
		this.k=k;
		this.consistency=consistency;
	}
	
	protected void takeAdditionalFromOne(String[] spl) {
		startReplica=spl[4];
	}

	protected void initServer() {
		this.server=new ReplicationServer(false,start,end,k,startReplica,end,consistency);
	}
	
	protected void processMessage(String newMessage){
		console.logEntry();
		console.log("newMessage:" +newMessage);
		
		if(isItaBasicMessage(newMessage))
			return;	
				
		else if (isItAnotherMessage(newMessage))
			return;
				

		else{
			query(newMessage);
		}
	}
	
	protected boolean isItAnotherMessage(String newMessage){
		if(newMessage.startsWith("NEWLOW")){
			//New Range due to new Node in network
			String[] spl=newMessage.split("-",2);
			updateStart(spl[1]);

			String reply=server.action(newMessage);
			if(newMessage.startsWith("NEWLOW-")){
				// this means range become smaller (new came) so let`s send data at prev (=new)
				sendMessage(spl[2],reply);
			}
			else{
				
			}
		}

		else if(newMessage.startsWith("NEWREPLICALOW-")){
			String[] spl=newMessage.split("-");
			updateReplicaStart(spl[1]);
			if(spl.length==3){
				String reply=server.action(newMessage);
				sendMessage(spl[2],reply);
			}
			else
				server.action(newMessage+"-0");
		}
		
		else if(newMessage.startsWith("SENDDATA-")){
			String reply = server.action(newMessage);
			sendMessage(reply.split("-")[2],reply);
		}
		
		else if(newMessage.startsWith("ADDREPLICA-")){
			server.action(newMessage);
		}
		else
			return false;
		
		return true;

		
	}
	
	private void updateReplicaStart(String replicaStart){
		this.startReplica=replicaStart;
		this.amiCut=(compareHash(startReplica,myShaId)>=0);
	}

	protected boolean isHere(String key){
		if (amiCut)
			return compareHash(startReplica,key)<0 || compareHash(key,end)<0 ;
		return compareHash(startReplica,key)<0  && compareHash(key,end)<0 ;
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
	
	public void main(String [] args){
		ReplicationRoutingServer prs=new ReplicationRoutingServer("127.0.0.1", 5002,"127.0.0.1",5000,2,0);
		prs.start();
	}
}
