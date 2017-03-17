package dht;

import java.io.BufferedReader;
import java.io.File;
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
import java.util.TreeMap;

public class ReplicationPrimaryRoutingServer extends PrimaryRoutingServer {
	private int replicationNumber;
	
	public ReplicationPrimaryRoutingServer(String myIp,int myPort, int replicationNumber, int replicationMethod)
	{
		super(myIp,myPort);
		console.log("PrimaryRoutingServer");
		this.replicationNumber=replicationNumber;
	}
	
	protected void updateNext(String receiver,String message)
	{
		console.logEntry();
		String network[]=receiver.split(":");
		Utilities.sendMessage(network[0],network[1],"NEWNEXT-"+message,receiver+" didn't respond! Exit",console);
		console.logExit();
	}
	
	
	public String informReplicas(String newhash,String dest)
	{
		console.logEntry();
		String lowvalue=newhash;
		int replicationNumber=this.replicationNumber;
		if(replicationNumber>networkIds.size()) replicationNumber=networkIds.size();
		Set<String> beforeSet=networkIds.headMap(newhash).keySet();
		Set<String> afterSet=networkIds.tailMap(newhash).keySet();
		Iterator<String> before=beforeSet.iterator(),cycleafter=beforeSet.iterator();
		Iterator<String> after=afterSet.iterator(),cyclebefore=afterSet.iterator();
		int numbefore=0, numafter=0;
		boolean flag=false;
		if(replicationNumber>beforeSet.size()) {
			numafter=replicationNumber-1-beforeSet.size();
			numbefore=beforeSet.size();
			flag=true;
		}
		else numbefore=replicationNumber-1;
		console.log(numbefore+"]!!!!!!!!!["+numafter);
		if(this.replicationNumber==replicationNumber)
		{
			if(flag)
			{
				int ignore=afterSet.size()-numafter;
				while(ignore-->0) lowvalue=cyclebefore.next();
			}
			while(numafter>0)
			{
				String replicalow=cyclebefore.next();
				String nexthash;
				if(after.hasNext()) nexthash=after.next();
				else nexthash=cycleafter.next();
				String destination=networkIds.get(nexthash);
				String[] network=destination.split(":");
				Utilities.sendMessage(network[0],network[1],"NEWREPLICALOW-"+replicalow+"-"+dest,destination+" does not answer",console);
				numafter--;
			}
			if(!flag)
			{
				int ignore=beforeSet.size()-numbefore;
				while(ignore-->0) lowvalue=before.next();
			}
			while(numbefore>0)
			{
				String replicalow=before.next();
				String nexthash;
				if(after.hasNext()) nexthash=after.next();
				else nexthash=cycleafter.next();
				String destination=networkIds.get(nexthash);
				String[] network=destination.split(":");
				Utilities.sendMessage(network[0],network[1],"NEWREPLICALOW-"+replicalow+"-"+dest,destination+" does not answer",console);
				numbefore--;
			}
		}
		else 
		{
			String destination,nexthash;
			if(after.hasNext()) nexthash=after.next();
			else nexthash=before.next();
			destination=networkIds.get(nexthash);
			String[] network=destination.split(":");
			Utilities.sendMessage(network[0],network[1],"SENDALLDATA-"+nexthash+"-"+dest,destination+" does not answer",console); //nodes are fewer than max replications
		}
		console.logExit();
		return lowvalue;
	}	
	
	private void nextData(String newhash,String dest)
	{
		console.logEntry();
		int replicationNumber=this.replicationNumber;
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
			Utilities.sendMessage(network[0],network[1],"NEWREPLICALOW-"+newhash,destination+" does not answer",console);
		}
		console.logExit();
	}
	
	private String nextReplica(String leavingkey)
	{
		console.logEntry();
		String result="";
		int replicationNumber=this.replicationNumber;
		if(replicationNumber>networkIds.size()) return "0";
		Set<String> beforeSet=networkIds.headMap(leavingkey).keySet();
		Set<String> afterSet=networkIds.tailMap(leavingkey).keySet();
		Iterator<String> before=beforeSet.iterator();
		Iterator<String> after=afterSet.iterator();
		String key;
		if(afterSet.size()<(replicationNumber))
		{
			int ignore=(replicationNumber-1)-afterSet.size();
			while(ignore-->0) before.next();
			key=before.next();
		}
		else
		{
			int ignore=(replicationNumber-1);
			while(ignore-->0) after.next();
			key=after.next();
		}
		result=networkIds.get(key);
		console.logExit();
		return result;
	}
	
	private void distributeReplicas(String key, String destination)
	{
		console.logEntry();
		if(replicationNumber>networkIds.size())	{
			console.logExit();
			return;
		}
		Set<String> beforeSet=networkIds.headMap(key).keySet();
		Set<String> afterSet=networkIds.tailMap(key).keySet();
		Iterator<String> before=beforeSet.iterator(),cycleafter=beforeSet.iterator();
		Iterator<String> after=afterSet.iterator(),cyclebefore=afterSet.iterator();
		int numbefore, numafter=0;
		if(beforeSet.size()<replicationNumber) numafter=replicationNumber-1-beforeSet.size();
		numbefore=replicationNumber-1-numafter;
		String[] network=destination.split(":");
		String replicalow="",replicahigh="";
		if(numafter>0)
		{
			int ignore=afterSet.size()-numafter;
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
			Utilities.sendMessage(network[0],network[1],"SENDREPLICA-"+replicalow+"-"+replicahigh+"-"+networkIds.get(nexthash),destination+" does not answer",console);
			numafter--;
		}
		if(numbefore>0)
		{
			int ignore;
			ignore=beforeSet.size()-numbefore;
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
			Utilities.sendMessage(network[0],network[1],"SENDREPLICA-"+replicalow+"-"+replicahigh+"-"+networkIds.get(nexthash),destination+" does not answer",console);
			numbefore--;
		}
		console.logExit();
		return;
	}
	
	protected void mergeRanges(String prev_key, String leaving_key,String next_key)
	{
		console.logEntry();
		String current_value;
		current_value=networkIds.get(next_key);
		String[] network=current_value.split(":");
		String next_low;
		next_low=prev_key;
		String dest=nextReplica(leaving_key);
		//if(next_key.equals(myShaId)) start=next_low;
		//else
		//{
		Utilities.sendMessage(network[0],network[1],"NEWLOW2-"+next_low+"-"+dest,"Id "+next_key+" didn't respond! Exit",console);
		//}
		console.logExit();
	}
	
	protected String newNode(String message)//message is the ip:port
	{
		console.logEntry();
		console.log("Replications are used");
		lastIdGiven++;
		String shaId=Utilities.hash(lastIdGiven);
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
		String replicaLow=informReplicas(shaId,message);
		networkIds.put(shaId,message);
		String result="";
		result=result+lastIdGiven+"-"+divideRanges(prev_key,shaId,next_key);
		result=result+"-"+current_value;
		nextData(shaId,message);
		result=result+"-"+replicaLow;
		console.logExit();
		return result;
	}
	
	protected String removeNode(String message)//message is the key 
	{
		console.logEntry();
		if(!networkIds.containsKey(message)) return "Nonexistent";
		String destination=networkIds.get(message);
		networkIds.remove(message);
		distributeReplicas(message,destination);
		String prev_key;
		if (networkIds.headMap(message).isEmpty()) prev_key=networkIds.lastKey();
		else prev_key=networkIds.headMap(message).lastKey();
		String prev_value=networkIds.get(prev_key);
		String next_key;
		if(networkIds.tailMap(message).isEmpty()) next_key=networkIds.firstKey();
		else next_key=networkIds.tailMap(message).firstKey();
		String curr_value=networkIds.get(next_key);
		updateNext(prev_value,curr_value);
		mergeRanges(prev_key,message,next_key);
		console.logExit();
		return "Removed";
	}
	
	protected String examineMessage(String message)
	{
		console.logEntry();
		String reply="";
		String[] split=message.split("-");
		if(split[0].equals("HELLO"))
		{
			console.log("Entering here sending this: "+split[1]);
			//***************************
			if(networkIds.isEmpty())
			{
				lastIdGiven=1;
				String shaId=Utilities.hash(lastIdGiven);
				networkIds.put(shaId,split[1]);
				reply=lastIdGiven+"-"+shaId+"-"+shaId+"-"+split[1]+"-"+shaId;
			}
			else
			//****************************	
				reply=newNode(split[1]);
		}
		else if(split[0].equals("Leaving"))
		{
			reply=removeNode(split[1]);
		}
		console.logExit();
		return reply;
	}

}

