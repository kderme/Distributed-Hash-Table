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

public class PrimaryRoutingServer extends RoutingServer {

	private int lastIdGiven;
	private SortedMap<String,String> networkIds=null;
	private ServerSocket SrvSocket;
	private Socket CltSocket;
	
	public PrimaryRoutingServer(String myIp,int myPort)
	{
		super(myIp,myPort,1,myIp,myPort);
		lastIdGiven=1;
		myId=lastIdGiven;
		myShaId=hash(myId);
		networkIds.put(myShaId,myIp+"-"+myPort);
		receiveMessages();
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
		try {
			CltSocket=new Socket(network[0],Integer.parseInt(network[1]));
			new PrintWriter(CltSocket.getOutputStream(), true).println("One-"+message);
			CltSocket.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println(receiver+" didn't respond! Exit");
			System.exit(1);
		}
	}
	
	private String divideRanges(String prev_key, String new_key, String next_key)
	{
		String result="";
		String current_value;
		Long new_low, new_high;
		new_low=Long.valueOf(prev_key)+1;
		new_high=Long.valueOf(new_key);
		current_value=networkIds.get(next_key);
		String[] network=current_value.split(":");
		Long next_low=Long.valueOf(new_key)+1;
		if (next_key.equals(myShaId)) start=next_low;
		else
		{	
			try {
				CltSocket=new Socket(network[0],Integer.parseInt(network[1]));
				new PrintWriter(CltSocket.getOutputStream(), true).println("NewLow-"+next_low.toString());
				CltSocket.close();
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("Id "+next_key+" didn't respond! Exit");
				System.exit(1);
			}
		}
		result=new_low.toString()+"-"+new_high.toString();
		return result;
	}
	
	private void mergeRanges(String prev_key, String next_key)
	{
		String current_value;
		current_value=networkIds.get(next_key);
		String[] network=current_value.split(":");
		Long next_low;
		next_low=Long.valueOf(prev_key)+1;
		if(next_key.equals(myShaId)) start=next_low;
		else
		{
			try {
				CltSocket=new Socket(network[0],Integer.parseInt(network[1]));
				new PrintWriter(CltSocket.getOutputStream(), true).println("NewLow-"+next_low.toString());
				CltSocket.close();
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("Id "+next_key+" didn't respond! Exit");
				System.exit(1);
			}
		}
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
		networkIds.put(shaId,message);
		String result="";
		result=result+lastIdGiven+"-"+divideRanges(prev_key,shaId,next_key);
		result=result+"-"+current_value;
		return result;
	}
	
	public String removeNode(String message)//message is the key 
	{
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
