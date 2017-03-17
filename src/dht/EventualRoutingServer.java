package dht;

import java.util.ArrayList;

public class EventualRoutingServer extends ReplicationRoutingServer{
	
	public EventualRoutingServer(String myIp, int myPort, String oneIp, int onePort, int k) {
		super(myIp, myPort, oneIp, onePort, k, 1);
		console.log("Linerization Routing Server");
	}
	
	protected void query(String newMessage){
		String sendMessage;
		if(newMessage.startsWith("#")){
			//writing
			sendMessage=newMessage;
			sendMessage=newMessage=newMessage.split("#",3)[2];
			newMessage=newMessage.split("@")[2];
			
			String [] message=newMessage.split(",");
			String key=message[0];	//TODO
			boolean isHere=isHere(key);
			if (isHere) 
			{
				server.action(newMessage);
				console.log("This one seems fine. Checking next with : "+sendMessage);
				outNext.println("#ANSWER-"+sendMessage.split("@",3)[1].split("/")[1]+"-OK#"+sendMessage);	
			}
			else
			{
				console.log("Checked all Replicas. All Done");
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
				if(!parts[0].equals("*")) parts[0]=Utilities.hash(parts[0]);
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
				console.log("Not my message:"+newMessage);
				outNext.println(sendMessage);
				console.log("Sending to next: "+sendMessage);
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
						console.log("Sending to next node: "+sendMessage);
						if(outNext!=null )outNext.println(sendMessage);
						else
						{
							String answer=server.action(newMessage);
							String token="ANSWER*";
							this.numberOfNodes.put(Integer.valueOf(sendBack[1]),new Integer(1));
							Utilities.sendMessage(sendBack[0], token+"-"+sendBack[1]+"-"+answer,console);
						}
					}
					else
					{
						String answer=server.action(newMessage);
						//	send back reply
						String [] sendBack=sendMessage.split("@")[1].split("/",2);
						String token="ANSWER*";
						sendMessage="@"+sendBack[0]+"/"+sendBack[1]+"@"+message[0]+","+message[1]+","+(Integer.parseInt(message[2])+1);
						if(key.equals("*") && !sendBack[0].equals(myIp+":"+myPort)) {
							console.log("Sending to next node: "+sendMessage);
							outNext.println(sendMessage);
						}
						else this.numberOfNodes.put(Integer.valueOf(sendBack[1]),Integer.parseInt(message[2]));
						Utilities.sendMessage(sendBack[0], token+"-"+sendBack[1]+"-"+answer,console);
					}
					return;
				}
				
				//**********************************************
				String answer=server.action(newMessage);
				String [] sendBack=sendMessage.split("@")[1].split("/",2);
				console.log("Owner of data. Sending answer:"+answer);
				Utilities.sendMessage(sendBack[0],"ANSWER-"+sendBack[1]+"-"+answer,console);
				return;
			}
			
			//We haven`t found a # header and its here
			boolean isMine=isMine(key);
			if(!isMine){
				console.log("Not master of data:"+newMessage);
				outNext.println(sendMessage);
				console.log("Sending to next: "+sendMessage);
				return;
			}
			String answer=server.action(newMessage);
			String [] sendBack=sendMessage.split("@")[1].split("/",2);
			String token="ANSWER";
			if(answer.split("%").length>1) sendMessage=sendMessage+"%"+answer.split("%")[1];
			outNext.println("#"+token+"-"+sendBack[1]+"-"+answer+"#"+sendMessage);
			Utilities.sendMessage(sendBack[0],"ANSWER-"+sendBack[1]+"-OK",console);
			return;
		}
	}
}

