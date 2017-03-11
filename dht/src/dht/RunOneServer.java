package dht;

import java.util.Random;

public class RunOneServer {

	public static void main(String[] args) {
		int replicationNumber=1;
		Random rand=new Random();
		RoutingServer rs;
		if(replicationNumber==1)rs= new RoutingServer("127.0.0.1",4000+rand.nextInt(1500),"127.0.0.1",4000);
		else rs=new ReplicationRoutingServer("127.0.0.1",4000+rand.nextInt(1500),"127.0.0.1",4000,replicationNumber,0);	
		rs.start();
			
		}

}
