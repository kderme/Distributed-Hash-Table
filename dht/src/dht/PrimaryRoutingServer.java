package dht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.SortedMap;
import java.util.TreeMap;

public class PrimaryRoutingServer extends RoutingServer {

	protected int lastIdGiven;
	protected SortedMap<String,String> networkIds=null;
	private ServerSocket SrvSocket;
	private Socket CltSocket;
	protected boolean isprimaryRunning;
	
	public PrimaryRoutingServer(String myIp,int myPort,String oneIp,int onePort)
	{
		super(myIp,myPort,oneIp,onePort);
		//lastIdGiven=1;
		//myId=lastIdGiven;
		//myShaId=hash(myId);
		networkIds=new TreeMap<String,String>();
		//networkIds.put(myShaId,myIp+":"+myPort);
		lastIdGiven=0;
		isprimaryRunning=false;
	}
	
	public void run(){
		if(!isprimaryRunning) {
			isprimaryRunning=true;
			receiveMessages();
		}
		else
		{
			super.run();
		}
	}
	
	protected void sendMessage(String Ip, String port, String message, String errorMessage)
	{
		console.logEntry();
		System.out.println("[One]: Sending message: "+message);
		System.out.println("to: "+Ip+":"+port);
		try{	
			CltSocket=new Socket(Ip,Integer.parseInt(port));
			new PrintWriter(CltSocket.getOutputStream(), true).println(message);
			CltSocket.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println(errorMessage);
			System.exit(1);
		}
		console.logExit();
	}
	
	
	protected void receiveMessages()
	{
		console.logEntry(); 
		try {
			   System.out.println("Connecting to "+this.oneIp+" to port "+this.onePort);
			   SrvSocket=new ServerSocket(this.onePort,5,InetAddress.getByName(this.oneIp));
			   while(true){
				  
					   Socket socket=SrvSocket.accept();
					   InputStreamReader inputStreamReader=new InputStreamReader(socket.getInputStream());
					   BufferedReader bufferedReader =new BufferedReader(inputStreamReader);
					   String message=bufferedReader.readLine();
					   System.out.println("Got this: "+message);
					   
					   String reply=examineMessage(message);
					   System.out.println("Answering with this: "+reply);
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
				console.logExit();
			} catch (IOException e) {
				System.out.println("Shouldnt close but closed");
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
		sendMessage(network[0],network[1],"NEWNEXT-"+message,receiver+" didn't respond! Exit");
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
			sendMessage(network[0],network[1],"NEWLOW-"+next_low+"-"+new_pos,"Id "+next_key+" didn't respond! Exit");
		//}
		result=new_low+"-"+new_high;
		console.logExit();
		return result;
	}
	
	protected void mergeRanges(String prev_key, String next_key)
	{
		console.logEntry();
		String current_value;
		current_value=networkIds.get(next_key);
		String[] network=current_value.split(":");
		String next_low;
		next_low=prev_key;
		if(next_key.equals(myShaId)) start=next_low;
		else
		{
			sendMessage(network[0],network[1],"NEWLOW2-"+next_low,"Id "+next_key+" didn't respond! Exit");
		}
		console.logExit();
	}
	
	protected String newNode(String message)//message is the ip:port
	{
		console.logEntry();
		System.out.println("No Replications used");
		lastIdGiven++;
		String shaId=hash(lastIdGiven);
		String prev_key;
		String current_value="";
		if(networkIds.headMap(shaId).isEmpty()) prev_key=networkIds.lastKey();
		else prev_key=networkIds.headMap(shaId).lastKey();
		current_value=networkIds.get(prev_key);
		System.out.println("We are Calling updateNext With parameters: "+current_value+" and "+message);
		updateNext(current_value,message);
		String next_key;
		if(networkIds.tailMap(shaId).isEmpty()) next_key=networkIds.firstKey();
		else next_key=networkIds.tailMap(shaId).firstKey();
		current_value=networkIds.get(next_key);
		System.out.println("Inserting in map: "+shaId+", "+message);
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
		networkIds.remove(message);
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
			System.out.println("Entering here sending this: "+split[1]);
			//***************************
			if(networkIds.isEmpty())
			{
				lastIdGiven=1;
				String shaId=hash(lastIdGiven);
				networkIds.put(shaId,split[1]);
				reply=lastIdGiven+"-"+shaId+"-"+shaId+"-"+split[1];
			}
			else
			//****************************	
				reply=newNode(split[1]);
		}
		else if(split[0].equals("BYEFROM"))
		{
			reply=removeNode(split[1]);
		}
		console.logExit();
		return reply;
	}
}
