package dht;

public class Runner {

	public static void main(String[] args)
	{
		int replicationNumber=3;
		int consistency=0;
		RoutingServer rprs;
		if(replicationNumber==1) rprs= new PrimaryRoutingServer("127.0.0.1",4001,"127.0.0.1",4000);
		else rprs=new ReplicationPrimaryRoutingServer("127.0.0.1",4001,"127.0.0.1",4000,replicationNumber,consistency);	
		rprs.start();
		new Thread(rprs).start();
		return;
	}

}
