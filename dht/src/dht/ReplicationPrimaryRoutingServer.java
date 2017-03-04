package dht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedMap;

public class ReplicationPrimaryRoutingServer extends PrimaryRoutingServer {

	private int lastIdGiven;
	private SortedMap<String,String> networkIds=null;
	private ServerSocket SrvSocket;
	private Socket CltSocket;
	private int replicationNumber;
	
	public ReplicationPrimaryRoutingServer(String myIp,int myPort, int replicationNumber)
	{
		super(myIp,myPort);
		this.replicationNumber=replicationNumber;
		lastIdGiven=1;
		myId=lastIdGiven;
		myShaId=hash(myId);
		networkIds.put(myShaId,myIp+"-"+myPort);
		receiveMessages();
	}
	
	private void sendMessage(String Ip, String port, String message, String errorMessage)
	{
		try{	
			CltSocket=new Socket(Ip,Integer.parseInt(port));
			new PrintWriter(CltSocket.getOutputStream(), true).println(message);
			CltSocket.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println(errorMessage);
			System.exit(1);
		}
	}
	
	private void receiveMessages()
	{
		 try {
			   System.out.println("Connecting to "+this.myIp+" to port "+this.myPort);
			   SrvSocket=new ServerSocket(this.myPort,5,InetAddress.getByName(this.myIp));
			   while(true){
				  
					   Socket socket=SrvSocket.accept();
					   InputStreamReader inputStreamReader=new InputStreamReader(socket.getInputStream());
					   BufferedReader bufferedReader =new BufferedReader(inputStreamReader);
					   String message=bufferedReader.readLine();
					   System.out.println("Got this: "+message);
					   
					   String reply=examineMessage(message);
					   
					   PrintStream PS=new PrintStream(socket.getOutputStream());
					   PS.println(reply);
					   
			   }
			   		
			   } catch (IOException e) {
					e.printStackTrace();
					System.out.println("Socket "+this.myPort+" Closed. Exiting...");
					
					return;
				}
			   finally{
				   try {
					SrvSocket.close();
				} catch (IOException e) {
					System.out.println("Shouldnt close but closed");
					e.printStackTrace();
				}
			   }
	}
	
	public void updateNext(String receiver,String message)
	{
		if(receiver.equals(myIp+":"+myPort))
		{
			try {
				connectWithNext(message);
				return;
			} catch (IOException e1) {
				System.out.println("One could not connect with Next");
				e1.printStackTrace();
				System.exit(1);
			}
		}
		String network[]=receiver.split(":");
		sendMessage(network[0],network[1],"NEWNEXT-"+message,receiver+" didn't respond! Exit");
	}
	
	private String divideRanges(String prev_key, String new_key, String next_key)
	{
		String result="";
		String current_value;
		String new_low, new_high;
		new_low=prev_key;
		new_high=new_key;
		current_value=networkIds.get(next_key);
		String[] network=current_value.split(":");
		String next_low=new_key;
		String new_pos=networkIds.get(new_key);
		if (next_key.equals(myShaId)) start=next_low;
		else
		{	
			sendMessage(network[0],network[1],"NEWLOW-"+next_low+"-"+new_pos,"Id "+next_key+" didn't respond! Exit");
		}
		result=new_low+"-"+new_high;
		return result;
	}
	
	private void mergeRanges(String prev_key, String next_key)
	{
		String current_value;
		current_value=networkIds.get(next_key);
		String[] network=current_value.split(":");
		String next_low;
		next_low=prev_key;
		if(next_key.equals(myShaId)) start=next_low;
		else
		{
			sendMessage(network[0],network[1],"NEWLOW-"+next_low,"Id "+next_key+" didn't respond! Exit");
		}
	}
	
	public void informReplicas(String newhash,String dest)
	{
		int replicationNumber=this.replicationNumber;
		if(replicationNumber>networkIds.size()) replicationNumber=networkIds.size();
		Set<String> beforeSet=networkIds.headMap(newhash).keySet();
		Set<String> afterSet=networkIds.tailMap(newhash).keySet();
		Iterator<String> before=beforeSet.iterator(),cycleafter=beforeSet.iterator();
		Iterator<String> after=afterSet.iterator(),cyclebefore=afterSet.iterator();
		int numbefore, numafter=0;
		if(beforeSet.size()<replicationNumber) numafter=replicationNumber-beforeSet.size();
		numbefore=replicationNumber-numafter;
		if(this.replicationNumber==replicationNumber)
		{
			if(numafter>0)
			{
				int ignore=afterSet.size()-numafter;
				while(ignore-->0) cyclebefore.next();
			}
			while(numafter>0)
			{
				String replicalow=cyclebefore.next();
				String nexthash;
				if(after.hasNext()) nexthash=after.next();
				else nexthash=cycleafter.next();
				String destination=networkIds.get(nexthash);
				String[] network=destination.split(":");
				sendMessage(network[0],network[1],"NEWREPLICALOW"+replicalow+"-"+dest,destination+" does not answer");
				numafter--;
			}
			if(numbefore>0)
			{
				int ignore=beforeSet.size()-numbefore;
				while(ignore-->0) before.next();
			}
			while(numbefore>0)
			{
				String replicalow=before.next();
				String nexthash;
				if(after.hasNext()) nexthash=after.next();
				else nexthash=cycleafter.next();
				String destination=networkIds.get(nexthash);
				String[] network=destination.split(":");
				sendMessage(network[0],network[1],"NEWREPLICALOW"+replicalow+"-"+dest,destination+" does not answer");
				numbefore--;
			}
		}
		else 
		{
			String destination;
			if(after.hasNext()) destination=after.next();
			else destination=before.next();
			String[] network=destination.split(":");
			sendMessage(network[0],network[1],"SENDALLDATA-"+newhash+"-"+dest,destination+" does not answer"); //nodes are fewer than max replications
		}
	}	
	
	private void nextReplica(String newhash,String dest)
	{
		int replicationNumber=this.replicationNumber;
		if(replicationNumber>networkIds.size()) replicationNumber=networkIds.size();
		if(networkIds.size()>=(replicationNumber+1))
		{
			Set<String> beforeSet=networkIds.headMap(newhash).keySet();
			Set<String> afterSet=networkIds.tailMap(newhash).keySet();
			Iterator<String> before=beforeSet.iterator();
			Iterator<String> after=afterSet.iterator();
			String key;
			if(afterSet.size()<(replicationNumber+1))
			{
				int ignore=(replicationNumber)-afterSet.size();
				while(ignore-->0) before.next();
				key=before.next();
			}
			else
			{
				int ignore=(replicationNumber);
				while(ignore-->0) after.next();
				key=after.next();
			}
			String destination=networkIds.get(key);
			String[] network=destination.split(":");
			sendMessage(network[0],network[1],"NEWREPLICALOW"+newhash+"-"+dest,destination+" does not answer");
		}
		//send message to next of new to get former data as replicas if he has not already done so	
	}
	
	private void distributeReplicas(String key)
	{
		int replicationNumber=this.replicationNumber;
		if(replicationNumber>networkIds.size())	replicationNumber=networkIds.size();
		if(this.replicationNumber==replicationNumber)
		{
			Set<String> beforeSet=networkIds.headMap(key).keySet();
			Set<String> afterSet=networkIds.tailMap(key).keySet();
			Iterator<String> before=beforeSet.iterator(),cycleafter=beforeSet.iterator();
			Iterator<String> after=afterSet.iterator(),cyclebefore=afterSet.iterator();
			int numbefore, numafter=0;
			if(beforeSet.size()<replicationNumber) numafter=replicationNumber-beforeSet.size();
			numbefore=replicationNumber-numafter;
			String destination=networkIds.get(key);
			String[] network=destination.split(":");
			String replicalow="",replicahigh="";
			if(numafter>0)
			{
				int ignore=afterSet.size()+1-numafter;
				while(ignore-->0) 
				{
					replicalow=replicahigh;
					replicahigh=cyclebefore.next();
				}
			}
			while(numafter>0)
			{
				replicalow=replicahigh;
				replicahigh=cyclebefore.next();
				String nexthash;
				if(after.hasNext()) nexthash=after.next();
				else nexthash=cycleafter.next();
				sendMessage(network[0],network[1],"SENDREPLICA"+replicalow+"-"+replicahigh+"-"+networkIds.get(nexthash),destination+" does not answer");
				numafter--;
			}
			if(numbefore>0)
			{
				int ignore;
				if(replicalow=="") ignore=beforeSet.size()+1-numbefore;
				else ignore=beforeSet.size()-numbefore;
				while(ignore-->0) 
				{
					replicalow=replicahigh;
					replicahigh=before.next();
				}
			}
			while(numbefore>0)
			{
				replicalow=replicahigh;
				replicahigh=before.next();
				String nexthash;
				if(after.hasNext()) nexthash=after.next();
				else nexthash=cycleafter.next();
				sendMessage(network[0],network[1],"SENDREPLICA"+replicalow+"-"+replicahigh+"-"+networkIds.get(nexthash),destination+" does not answer");
				numbefore--;
			}
		}
		//next should send his new data as replica to farthest node if he has not already done so
	}
	
	public String newNode(String message)//message is the ip:port
	{
		lastIdGiven++;
		String shaId=hash(lastIdGiven);
		String prev_key;
		String current_value="";
		if(networkIds.headMap(shaId).isEmpty()) prev_key=networkIds.lastKey();
		else prev_key=networkIds.headMap(shaId).lastKey();
		current_value=networkIds.get(prev_key);
		updateNext(current_value,message);
		String next_key;
		if(networkIds.tailMap(shaId).isEmpty()) next_key=networkIds.firstKey();
		else next_key=networkIds.tailMap(shaId).firstKey();
		current_value=networkIds.get(next_key);
		informReplicas(shaId,message);
		networkIds.put(shaId,message);
		String result="";
		result=result+lastIdGiven+"-"+divideRanges(prev_key,shaId,next_key);
		result=result+"-"+current_value;
		nextReplica(shaId,message);
		return result;
	}
	
	public String removeNode(String message)//message is the key 
	{
		if(!networkIds.containsKey(message)) return "Nonexistent";
		networkIds.remove(message);
		distributeReplicas(message);
		String prev_key;
		if (networkIds.headMap(message).isEmpty()) prev_key=networkIds.lastKey();
		else prev_key=networkIds.headMap(message).lastKey();
		String prev_value=networkIds.get(prev_key);
		String next_key;
		if(networkIds.tailMap(message).isEmpty()) next_key=networkIds.firstKey();
		else next_key=networkIds.tailMap(message).firstKey();
		String curr_value=networkIds.get(next_key);
		updateNext(prev_value,curr_value);
		mergeRanges(prev_key,next_key);
		return "Removed";
	}
	
	public String examineMessage(String message)
	{
		String reply="";
		String[] split=message.split("-");
		if(split[0].equals("Hello"))
		{
			reply=newNode(split[1]);
		}
		else if(split[0].equals("ByeFrom"))
		{
			reply=removeNode(split[1]);
		}
		return reply;
	}
}
