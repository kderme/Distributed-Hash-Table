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
import java.util.LinkedHashMap;
import java.util.Set;

public class PrimaryRoutingServer extends RoutingServer {

	private int lastIdGiven;
	private LinkedHashMap<String,String> networkIds=null;
	private int nodesNumber;
	private ServerSocket SrvSocket;
	private Socket CltSocket;
	
	public PrimaryRoutingServer(String myIp,int myPort)
	{
		super(myIp,myPort,1,myIp,myPort);
		nodesNumber=1;
		lastIdGiven=1;
		networkIds=new LinkedHashMap<String,String>();
		receiveMessages();
	}
	
	private String calculateNewRanges()
	{
		long range=(((long)Math.pow(2,32))-1)/nodesNumber;
		long low=0;
		long high;
		String result="";
		Set<String> allIds = networkIds.keySet();
		Iterator<String> iter=allIds.iterator();
		String current_key,current_value;
		//considering the master node never leaves
		this.start=low;
		high=low+range;
		this.end=high;
		while(iter.hasNext())
		{
			low=high+1;
			current_key=iter.next();
			current_value=networkIds.get(current_key);
			if(iter.hasNext()) 
			{
				high=low+range;
			}
			else high=((long)Math.pow(2,32))-1; 
			String[] network=current_value.split(":");
			try {
				CltSocket=new Socket(network[0],Integer.parseInt(network[2]));
				new PrintWriter(CltSocket.getOutputStream(), true).println("NewRange-"+low+"-"+high);
				CltSocket.close();
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("Id "+current_key+" din`t respond! Exit");
				System.exit(1);
			}
		}
		result=low+"-"+high;
		return result;
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
	
	public String newNode(String message)
	{
		lastIdGiven++;
		nodesNumber++;
		networkIds.put(Integer.valueOf(lastIdGiven).toString(),message);
		String result="";
		result=result+lastIdGiven+"-"+calculateNewRanges();
		result=result+"-"+this.myIp+":"+this.myPort;
		return result;
	}
	
	public String removeNode(String message)
	{
		if(!networkIds.containsKey(message)) return "Nonexistent";
		nodesNumber--;
		networkIds.remove(message);
		calculateNewRanges();
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
