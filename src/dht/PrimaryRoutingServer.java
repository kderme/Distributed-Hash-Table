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
import java.util.SortedMap;
import java.util.TreeMap;

public class PrimaryRoutingServer extends Thread {

	protected String myIp;
	protected int myPort;
	protected RoutingServer rs=null;
	protected Console console;
	
	protected int lastIdGiven=0;
	protected SortedMap<String,String> networkIds=new TreeMap<String,String>();;
	public PrimaryRoutingServer(String myIp,int myPort){
		this.myIp=myIp;
		this.myPort=myPort;
		this.console=new Console(myPort+"","logs"+File.separator+myPort+".txt");
		console.log("ReplicationPrimaryRoutingServer");
	}
	
	public void run(){
		receiveMessages();
	}	
		
	protected void receiveMessages()
	{
		console.logEntry();
		ServerSocket SrvSocket=null;
		try {
			   console.log("Connecting to "+this.myIp+" to port "+this.myPort);
			   SrvSocket=new ServerSocket(this.myPort,5,InetAddress.getByName(this.myIp));
			   while(true){
				  
					   Socket socket=SrvSocket.accept();
					   InputStreamReader inputStreamReader=new InputStreamReader(socket.getInputStream());
					   BufferedReader bufferedReader =new BufferedReader(inputStreamReader);
					   String message=bufferedReader.readLine();
					   console.log("Got this: "+message);
					   
					   String reply=examineMessage(message);
					   console.log("Answering with this: "+reply);
					   PrintStream PS=new PrintStream(socket.getOutputStream());
					   PS.println(reply);
					   
			   }
		} catch (IOException e) {
			e.printStackTrace();
			console.log("Socket "+this.myPort+" Closed. Exiting...");
			return;
		}
		finally{
			try {
				SrvSocket.close();
				console.logExit();
			} catch (IOException e) {
				console.log("Shouldnt close but closed");
				e.printStackTrace();
			}
		}
	}
	
	protected void updateNext(String receiver,String message)
	{
		console.logEntry();
		/*
		if(receiver.equals(myIp+":"+myPort))
		{
			connectWithNext(message);
			return;
		}
		*/
		String network[]=receiver.split(":");
		Utilities.sendMessage(network[0],network[1],"NEWNEXT-"+message,receiver+" didn't respond! Exit",console);
		console.logExit();
	}
	
	protected String divideRanges(String prev_key, String new_key, String next_key)
	{
		console.logEntry();
		String result="";
		String current_value;
		String new_low, new_high;
		new_low=prev_key;
		new_high=new_key;
		current_value=networkIds.get(next_key);
		String[] network=current_value.split(":");
		String next_low=new_key;
		String new_pos=networkIds.get(new_key);
		//if (next_key.equals(myShaId)) start=next_low;
		//else
		//{	
			Utilities.sendMessage(network[0],network[1],"NEWLOW-"+next_low+"-"+new_pos,"Id "+next_key+" didn't respond! Exit",console);
		//}
		result=new_low+"-"+new_high;
		console.logExit();
		return result;
	}
	
	protected void mergeRanges(String prev_key,String leaving_location, String next_key)
	{
		console.logEntry();
		String current_value;
		current_value=networkIds.get(next_key);
		String[] network=leaving_location.split(":");
		String next_low;
		next_low=prev_key;
		//if(next_key.equals(myShaId)) start=next_low;
		//else
		//{
			Utilities.sendMessage(network[0],network[1],"NEWLOW2-"+next_low+"-"+current_value,"Id "+next_key+" didn't respond! Exit",console);
		//}
		console.logExit();
	}
	
	protected String newNode(String message)//message is the ip:port
	{
		console.logEntry();
		console.log("No Replications used");
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
		networkIds.put(shaId,message);
		String result="";
		result=result+lastIdGiven+"-"+divideRanges(prev_key,shaId,next_key);
		result=result+"-"+current_value;
		console.logExit();
		return result;
	}
	
	protected String removeNode(String message)//message is the key 
	{
		console.logEntry();
		if(!networkIds.containsKey(message)) return "Nonexistent";
		String remove_location=networkIds.get(message);
		networkIds.remove(message);
		String prev_key;
		if (networkIds.headMap(message).isEmpty()) prev_key=networkIds.lastKey();
		else prev_key=networkIds.headMap(message).lastKey();
		String prev_value=networkIds.get(prev_key);
		String next_key;
		if(networkIds.tailMap(message).isEmpty()) next_key=networkIds.firstKey();
		else next_key=networkIds.tailMap(message).firstKey();
		String curr_value=networkIds.get(next_key);
		System.out.println("Connect "+prev_value+" to "+curr_value);
		updateNext(prev_value,curr_value);
		mergeRanges(prev_key,remove_location,next_key);
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
			if(networkIds.isEmpty())
			{
				lastIdGiven=1;
				String shaId=Utilities.hash(lastIdGiven);
				networkIds.put(shaId,split[1]);
				reply=lastIdGiven+"-"+shaId+"-"+shaId+"-"+split[1];
			}
			else
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
