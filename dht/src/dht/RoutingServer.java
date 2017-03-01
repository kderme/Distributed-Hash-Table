package dht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import org.apache.commons.codec.digest.DigestUtils;

public class RoutingServer {
	private int myId;
	private String myShaId;
	private String myIp;
	private int myPort;
	
	private Socket socketNeo, socketPrev, socketNext;
	private PrintWriter outNeo, outPrev, outNext;
	private BufferedReader inNeo, inPrev, inNext;;
	
	RoutingServer previous,next;
	
	
	private int sayHelloToOne (String oneIp, int onePort){
		
		try {
			socketNeo = new Socket(oneIp, onePort);
			outNeo= new PrintWriter(socketNeo.getOutputStream(), true);
			inNeo = new BufferedReader(
	                new InputStreamReader(socketNeo.getInputStream()));

			//inform One
			outNeo.println("Hello-"+myIp+":"+myPort);
			
			//take id
			String mySid = inNeo.readLine();
			myId = new Integer(mySid);
			myShaId = hash(myId);
			
			//take routing info
			String next []=inNeo.readLine().split("-");
			String prev []=inNeo.readLine().split("-");
			
			socketNext =new Socket(next[0],new Integer(next[1]));
			socketNext =new Socket(prev[0],new Integer(prev[1]));
			
			outNext= new PrintWriter(socketNext.getOutputStream(), true);
			inPrev = new BufferedReader(
	                new InputStreamReader(socketPrev.getInputStream()));
			
			
			
			return 1;
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("One din`t respond! You are now alone");
			return 0;
		}

	}
	
	private String hash(String s){
		return org.apache.commons.codec.digest.DigestUtils.sha1Hex("");
	}
	
	private String hash(int n){
		return hash(n+"");
	}
}
