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
		if(newMessage.startsWith("NEWLOW-")){
			//New Range due to new Node in network
			String[] spl=newMessage.split("-");
			updateStart(spl[1]);
			if(spl.length==3){
				// this means range become smaller (new came) so let`s send data at prev (=new)
				String reply=server.action("NEWLOW-"+start);
				sendMessage(spl[2],reply);
			}
		}
		else if(newMessage.startsWith("NEWLOW2-"))
		{
			String[] spl=newMessage.split("-");
			String reply=server.action(newMessage);
			if(!spl[2].equals("0"))sendMessage(spl[2],reply);
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
		
		else if(newMessage.startsWith("SENDDATA-") || newMessage.startsWith("SENDALLDATA-") || newMessage.startsWith("SENDREPLICA-")){
			String reply = server.action(newMessage);
			sendMessage(reply.split("-")[2],reply);
		}
		
		else if(newMessage.startsWith("ADDREPLICA-") || newMessage.startsWith("UPDATEREPLICA-")){
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
		if(key.equals("*")) return true;
		if (compareHash(startReplica,end)>=0)
			return compareHash(startReplica,key)<=0 || compareHash(key,end)<0 ;
		return compareHash(startReplica,key)<=0  && compareHash(key,end)<0 ;
	}

	protected boolean sendMessage(String ip, int port, String message){
		console.log("["+myIp+":"+myPort+"] Sending message: "+message);
		console.log("to: "+ip+":"+port);
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
	
	protected void query(String newMessage){
	if(consistency==0)
	{	
		String sendMessage;
		int n=0;
		if(newMessage.startsWith("##")){
			//reading
			String prevAnswer=newMessage.split("##",3)[1];
			sendMessage=newMessage=newMessage.split("##",3)[2];
			newMessage=newMessage.split("@")[2];
			
			String [] message=newMessage.split(",");
			String key=message[0];	//TODO
			boolean isHere=isHere(key);
			if (!isHere){
				//first who doesn't find it sends back result of previous
				console.log("["+myIp+":"+myPort+"]: Checked all Replicas. Final Answer: "+prevAnswer);
				sendMessage(sendMessage.split("@")[1].split("/")[0], prevAnswer);
			}
			else{
				//else send answer to next
				String answer=server.action(newMessage);
				//***********************************
				if(answer==null) System.out.println("This is the problem");
				if(answer==null && prevAnswer.split("-")[2].equals("null"))
				{ 
					console.log("["+myIp+":"+myPort+"]: This one seems fine. Checking next with : "+sendMessage);
					//***********************************	
					outNext.println("##ANSWER-"+prevAnswer.split("-")[1]+"-"+answer+"##"+sendMessage);
				}
				else {
					if(answer==null || prevAnswer.split("-")[2].equals("null"))
					{	
						console.log("["+myIp+":"+myPort+"]: An error occured. Sending from start: "+sendMessage);
						outNext.println(sendMessage);
					}
					else {
						if(answer.equals(prevAnswer.split("-")[2]))
						{
							console.log("["+myIp+":"+myPort+"]: This one seems fine. Checking next with : "+sendMessage);
							//***********************************	
							outNext.println("##ANSWER-"+prevAnswer.split("-")[1]+"-"+answer+"##"+sendMessage);
							
						}
						else {
							console.log("["+myIp+":"+myPort+"]: An error occured. Sending from start: "+sendMessage);
							outNext.println(sendMessage);
						}
					}
				}
			}
		}
		
		else if(newMessage.startsWith("#")){
			//writing
			sendMessage=newMessage;
			sendMessage=newMessage=newMessage.split("#",3)[2];
			newMessage=newMessage.split("@")[2];
			
			String [] message=newMessage.split(",");
			String key=message[0];	//TODO
			boolean isHere=isHere(key);
			if (isHere){
				String answer=server.action(newMessage);
				console.log("["+myIp+":"+myPort+"]: This one seems fine. Checking next with : "+sendMessage);
				outNext.println("#ANSWER-"+sendMessage.split("@",3)[1].split("/")[1]+"-OK#"+sendMessage);
				
			}
			else
			{
				console.log("["+myIp+":"+myPort+"]: Checked all Replicas. All Done");
				sendMessage(sendMessage.split("@",3)[1].split("/")[0],"ANSWER-"+sendMessage.split("@",3)[1].split("/")[1]+"-OK");
			}
		}
		else{
			
			//reading or writing hasn`t begun
			//create iport header
			String iport;
			int ClientId=0;
			if(newMessage.startsWith("@")){
				iport=newMessage.split("@",3)[1].split("/")[0];
				ClientId=Integer.parseInt(newMessage.split("@",3)[1].split("/")[1]);
				sendMessage=newMessage;
				newMessage=newMessage.split("@",3)[2];
			}
			else
			{
				iport=myIp+":"+myPort;						
				String[] parts=newMessage.split(",");
				if(!parts[0].equals("*")) parts[0]=hash(parts[0]);
				else data.put(currentClid,new ArrayList<String>());
				int i;
				newMessage=parts[0];
				for (i=1;i<parts.length;i++) newMessage=newMessage+","+parts[i];
				ht.put(currentClid,currentSC);
				ClientId=currentClid;
				currentClid++;
			}
			
			sendMessage="@"+iport+"/"+ClientId+"@"+newMessage;
			String [] message=newMessage.split(",");
			String key=message[0];	//TODO
			String query=message[1];
			
			boolean isHere=isHere(key);
			if(!isHere){
				console.log("["+myIp+":"+myPort+"]: Not my message:"+newMessage);
				outNext.println(sendMessage);
				return;
			}
			
			//it`s here
			if(query.equals("query")){
				//if it`s here and it`s a query send answer to next until its not here
				//then send it back
				//**********************************************
				if(key.equals("*"))
				{
					if(message.length==2)
					{
						String [] sendBack=sendMessage.split("@")[1].split("/",2);
						sendMessage=sendMessage+",1";
						this.numberOfNodes.put(Integer.valueOf(sendBack[1]),new Integer(0));
						console.log("["+myIp+":"+myPort+"]: Sending to next node: "+sendMessage);
						outNext.println(sendMessage);
					}
					else
					{
						String answer=server.action(newMessage);
						//	send back reply
						String [] sendBack=sendMessage.split("@")[1].split("/",2);
						String token="ANSWER*";
						sendMessage="@"+sendBack[0]+"/"+sendBack[1]+"@"+message[0]+","+message[1]+","+(Integer.parseInt(message[2])+1);
						if(key.equals("*") && !sendBack[0].equals(myIp+":"+myPort)) {
							console.log("["+myIp+":"+myPort+"]: Sending to next node: "+sendMessage);
							outNext.println(sendMessage);
						}
						else this.numberOfNodes.put(Integer.valueOf(sendBack[1]),Integer.parseInt(message[2]));
						sendMessage(sendBack[0], token+"-"+sendBack[1]+"-"+answer);
					}
					return;
				}
				
				//**********************************************
				boolean isMine=isMine(key);
				if (isMine){
					String answer=server.action(newMessage);
					String [] sendBack=sendMessage.split("@")[1].split("/",2);
					String token="ANSWER";
					console.log("["+myIp+":"+myPort+"]: Master of data. Sending:"+newMessage);
					outNext.println("##"+token+"-"+sendBack[1]+"-"+answer+"##"+sendMessage);
				}
				else
				{
					console.log("["+myIp+":"+myPort+"]: Not master of data. Transfering:"+newMessage);
					outNext.println(sendMessage);
				}
				return;
			}
			
			boolean isMine=isMine(key);
			if (isMine){
				//We haven`t found a # header and its here
				String answer=server.action(newMessage);
				String [] sendBack=sendMessage.split("@")[1].split("/",2);
				String token="ANSWER";
				outNext.println("#"+token+"-"+sendBack[1]+"-"+answer+"#"+sendMessage);
				return;
			}
			else{
				console.log("["+myIp+":"+myPort+"]: Not master of data:"+newMessage);
				outNext.println(sendMessage);
			}
		}
	}
	else
	{
		String sendMessage;
		int n=0;
		if(newMessage.startsWith("#")){
			//writing
			sendMessage=newMessage;
			sendMessage=newMessage=newMessage.split("#",3)[2];
			newMessage=newMessage.split("@")[2];
			
			String [] message=newMessage.split(",");
			String key=message[0];	//TODO
			boolean isHere=isHere(key);
			if (!sendMessage.split("@")[1].split("/")[0].equals(myIp+":"+myPort)){
				if(isHere) 
				{
					String answer=server.action(newMessage);
					console.log("["+myIp+":"+myPort+"]: This one seems fine. Checking next with : "+sendMessage);
				}
				outNext.println("#ANSWER-"+sendMessage.split("@",3)[1].split("/")[1]+"-OK#"+sendMessage);	
			}
			else
			{
				console.log("["+myIp+":"+myPort+"]: Checked all Replicas. All Done");
				return;
			}
		}
		else{
			
			//reading or writing hasn`t begun
			//create iport header
			String iport;
			int ClientId=0;
			if(newMessage.startsWith("@")){
				iport=newMessage.split("@",3)[1].split("/")[0];
				ClientId=Integer.parseInt(newMessage.split("@",3)[1].split("/")[1]);
				sendMessage=newMessage;
				newMessage=newMessage.split("@",3)[2];
			}
			else
			{
				iport=myIp+":"+myPort;						
				String[] parts=newMessage.split(",");
				if(!parts[0].equals("*")) parts[0]=hash(parts[0]);
				else data.put(currentClid,new ArrayList<String>());
				int i;
				newMessage=parts[0];
				for (i=1;i<parts.length;i++) newMessage=newMessage+","+parts[i];
				ht.put(currentClid,currentSC);
				ClientId=currentClid;
				currentClid++;
			}
			
			sendMessage="@"+iport+"/"+ClientId+"@"+newMessage;
			String [] message=newMessage.split(",");
			String key=message[0];	//TODO
			String query=message[1];
			
			boolean isHere=isHere(key);
			if(!isHere){
				console.log("["+myIp+":"+myPort+"]: Not my message:"+newMessage);
				outNext.println(sendMessage);
				return;
			}
			
			//it`s here
			if(query.equals("query")){
				//if it`s here and it`s a query send answer to next until its not here
				//then send it back
				//**********************************************
				if(key.equals("*"))
				{
					if(message.length==2)
					{
						String [] sendBack=sendMessage.split("@")[1].split("/",2);
						sendMessage=sendMessage+",1";
						this.numberOfNodes.put(Integer.valueOf(sendBack[1]),new Integer(0));
						console.log("["+myIp+":"+myPort+"]: Sending to next node: "+sendMessage);
						outNext.println(sendMessage);
					}
					else
					{
						String answer=server.action(newMessage);
						//	send back reply
						String [] sendBack=sendMessage.split("@")[1].split("/",2);
						String token="ANSWER*";
						sendMessage="@"+sendBack[0]+"/"+sendBack[1]+"@"+message[0]+","+message[1]+","+(Integer.parseInt(message[2])+1);
						if(key.equals("*") && !sendBack[0].equals(myIp+":"+myPort)) {
							console.log("["+myIp+":"+myPort+"]: Sending to next node: "+sendMessage);
							outNext.println(sendMessage);
						}
						else this.numberOfNodes.put(Integer.valueOf(sendBack[1]),Integer.parseInt(message[2]));
						sendMessage(sendBack[0], token+"-"+sendBack[1]+"-"+answer);
					}
					return;
				}
				
				//**********************************************
				String answer=server.action(newMessage);
				String [] sendBack=sendMessage.split("@")[1].split("/",2);
				String token="ANSWER";
				console.log("["+myIp+":"+myPort+"]: Owner of data. Sending answr:"+answer);
				sendMessage(sendBack[0],"ANSWER-"+sendBack[1]+"-"+answer);
				return;
			}
			
			//We haven`t found a # header and its here
			String answer=server.action(newMessage);
			String [] sendBack=sendMessage.split("@")[1].split("/",2);
			String token="ANSWER";
			outNext.println("#"+token+"-"+sendBack[1]+"-"+answer+"#"+sendMessage);
			sendMessage(sendBack[0],"ANSWER-"+sendBack[1]+"-OK");
			return;
		}
	}
	}
}
