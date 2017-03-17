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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
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
		console.log("Replication Routing Server");
	}
	
	protected void takeAdditionalFromOne(String[] spl) {
		startReplica=spl[4];
	}

	protected void initServer() {
		this.server=new ReplicationServer(false,start,end,k,startReplica,end,consistency,console);
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
		else if(newMessage.startsWith("NEWLOW2-"))
		{
			String[] spl=newMessage.split("-");
			String reply=server.action(newMessage);
			if(!spl[2].equals("0"))Utilities.sendMessage(spl[2],reply,console);
		}

		else if(newMessage.startsWith("NEWREPLICALOW-")){
			String[] spl=newMessage.split("-");
			updateReplicaStart(spl[1]);
			if(spl.length==3){
				String reply=server.action(newMessage);
				Utilities.sendMessage(spl[2],reply,console);
			}
			else
				server.action(newMessage+"-0");
		}
		
		else if(newMessage.startsWith("SENDDATA-") || newMessage.startsWith("SENDALLDATA-") || newMessage.startsWith("SENDREPLICA-")){
			leave_count++;
			if(newMessage.startsWith("SENDREPLICA")) console.log("With values of "+startReplica+" and "+end+"\nwe give "+newMessage.split("-")[1]+" and "+newMessage.split("-")[2]);
			String reply = server.action(newMessage);
			Utilities.sendMessage(reply.split("-")[2],reply,console);
		}
		
		else if(newMessage.startsWith("ADDREPLICA-") || newMessage.startsWith("UPDATEREPLICA-")){
			server.action(newMessage);
		}
		else
			return false;
		
		return true;		
	}
	
	protected boolean further_checking(){return (leave_count>=k);}
	
	private void updateReplicaStart(String replicaStart){
		this.startReplica=replicaStart;
		this.amiCut=(Utilities.compareHash(startReplica,myShaId)>=0);
	}

	protected boolean isHere(String key){
		if(key.equals("*")) return true;
		if (Utilities.compareHash(startReplica,end)>=0)
			return Utilities.compareHash(startReplica,key)<=0 || Utilities.compareHash(key,end)<0 ;
		return Utilities.compareHash(startReplica,key)<=0  && Utilities.compareHash(key,end)<0 ;
	}	
	
	public void main(String[] args)
	{
		int replicationNumber=1;
		RoutingServer rs;
		Random rand=new Random();
		if(replicationNumber==1)rs= new RoutingServer("127.0.0.1",4000+rand.nextInt(1500),"127.0.0.1",4000);
		else rs=new ReplicationRoutingServer("127.0.0.1",4000,"127.0.0.1",4000+rand.nextInt(1500),replicationNumber,0);	
		rs.start();
		return;
	}
}
