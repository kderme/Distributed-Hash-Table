package dht;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

public class SendQuery {

	private static void sendMessage(String Ip, String port, String message, String errorMessage)
	{
		System.out.println("[One]: Sending message: "+message);
		System.out.println("to: "+Ip+":"+port);
		try{	
			Socket CltSocket=new Socket(Ip,Integer.parseInt(port));
			new PrintWriter(CltSocket.getOutputStream(), true).println(message);
			CltSocket.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println(errorMessage);
			System.exit(1);
		}
	}
	
	public static void main(String[] args)
	{
		String ip="127.0.0.1";
		String port="4001";
		String message="6,query";
		String error="Error";
		sendMessage(ip,port,message,error);
		return;
	}
}
