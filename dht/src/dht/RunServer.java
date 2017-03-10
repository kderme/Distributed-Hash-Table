package dht;

import java.util.Random;

public class RunServer {

	public static void main(String[] args) throws InterruptedException {
		int replicationNumber=1;
		Random rand=new Random();
		RoutingServer rs;
		if(replicationNumber==1)rs= new RoutingServer("127.0.0.1",4543,"127.0.0.1",4000);
		else rs=new ReplicationRoutingServer("127.0.0.1",4543,"127.0.0.1",4000,replicationNumber,0);	
		rs.start();
		Thread.sleep(1500);
		if(replicationNumber==1)rs= new RoutingServer("127.0.0.1",4300,"127.0.0.1",4000);
		else rs=new ReplicationRoutingServer("127.0.0.1",4300,"127.0.0.1",4000,replicationNumber,0);	
		rs.start();
		Thread.sleep(1500);
		if(replicationNumber==1)rs= new RoutingServer("127.0.0.1",5130,"127.0.0.1",4000);
		else rs=new ReplicationRoutingServer("127.0.0.1",4000+5130,"127.0.0.1",4000,replicationNumber,0);	
		rs.start();
		Thread.sleep(1500);
		if(replicationNumber==1)rs= new RoutingServer("127.0.0.1",5054,"127.0.0.1",4000);
		else rs=new ReplicationRoutingServer("127.0.0.1",4000+5054,"127.0.0.1",4000,replicationNumber,0);	
		rs.start();
		Thread.sleep(1500);
		if(replicationNumber==1)rs= new RoutingServer("127.0.0.1",4167,"127.0.0.1",4000);
		else rs=new ReplicationRoutingServer("127.0.0.1",4000+4167,"127.0.0.1",4000,replicationNumber,0);	
		rs.start();
		Thread.sleep(1500);
		if(replicationNumber==1)rs= new RoutingServer("127.0.0.1",5289,"127.0.0.1",4000);
		else rs=new ReplicationRoutingServer("127.0.0.1",4000+5289,"127.0.0.1",4000,replicationNumber,0);	
		rs.start();
		}

}
