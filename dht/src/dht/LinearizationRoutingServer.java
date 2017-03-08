package dht;

public class LinearizationRoutingServer extends ReplicationRoutingServer{
	
	public LinearizationRoutingServer(String myIp, int myPort, String oneIp, int onePort, int k) {
		super(myIp, myPort, oneIp, onePort, k, 0);
	}

	
	protected void query(String newMessage){
		String sendMessage;
		System.out.println("%%%%%%%%%%%%%%%%%");
		int n=0;
		if(newMessage.startsWith("##")){
			//reading
			String prevAnswer=newMessage=newMessage.split("##",3)[1];
			sendMessage=newMessage;
			newMessage=newMessage.split("@")[2];
			
			String [] message=newMessage.split(",");
			String key=message[0];	//TODO
			String query=message[1];
			boolean isHere=isHere(key);
			if (!isHere){
				//first who doesn`t find it sends back result of previous
				sendMessage(sendMessage.split("@")[1], prevAnswer);
			}
			else{
				//else send answer to next
				String answer=server.action(sendMessage.split("@",4)[2]);
				//***********************************
				if(!answer.equals(prevAnswer)) outNext.println(sendMessage);
				else
				//***********************************	
					outNext.println("##"+answer+"##"+sendMessage);
			}
		}
		
		else if(newMessage.startsWith("#")){
			//writing
			sendMessage=newMessage;
			newMessage=newMessage.split("@",3)[2];
			
			String [] message=newMessage.split(",");
			String key=message[0];	//TODO
			String query=message[1];
			boolean isHere=isHere(key);
			if (isHere){
				String answer=server.action(sendMessage.split("@",4)[2]);
				outNext.println(newMessage);
				
			}
			else
				sendMessage(sendMessage.split("@",3)[1],"OK");
		}
		else{
			
			//reading or writing hasn`t begun
			//create iport header
			String iport;
			if(newMessage.startsWith("@")){
				iport=newMessage.split("@",3)[1];
				newMessage=newMessage.split("@",3)[2];
			}
			else
				iport=myIp+":"+myPort;						
			
			sendMessage="@"+iport+"@"+newMessage;
			String [] message=newMessage.split(",");
			String key=message[0];	//TODO
			String query=message[1];
			
			boolean isHere=isHere(key);
			if(!isHere){
				outNext.println(sendMessage);
				return;
			}
			
			//it`s here
			if(query.equals("query")){
				//if it`s here and it`s a querry send answer to next until its not here
				//then send it back
				String answer=server.action(newMessage);
				outNext.println("##"+answer+"##"+sendMessage);
				return;
			}
			
			boolean isMine=isMine(key);
			if (isMine){
				//We haven`t found a # header and its here
				String answer=server.action(newMessage);
				outNext.println("#"+sendMessage);
			}
			else{
				outNext.println(sendMessage);
			}
		}
	}
}
