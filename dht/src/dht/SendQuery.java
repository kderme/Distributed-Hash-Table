package dht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class SendQuery {

	private static Socket sendMessage(String Ip, String port, String message, String errorMessage)
	{
		System.out.println("[One]: Sending message: "+message);
		System.out.println("to: "+Ip+":"+port);
		Socket CltSocket=null;
		try{	
			CltSocket=new Socket(Ip,Integer.parseInt(port));
			new PrintWriter(CltSocket.getOutputStream(), true).println(message);
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println(errorMessage);
			System.exit(1);
		}
		return CltSocket;
	}
	
	public static void main(String[] args) throws InterruptedException
	{
		String ip="127.0.0.1";
		String port="4001";
		String message;
		Socket CltSocket;
		message="15,insert,life";
		String error="Error";
		CltSocket=sendMessage(ip,port,message,error);
		/*Thread.sleep(1500);
		message="22,insert,fire";
		CltSocket=sendMessage(ip,port,message,error);
		Thread.sleep(1500);
		port="4543";
		message="4,insert,red";
		CltSocket=sendMessage(ip,port,message,error);
		Thread.sleep(1500);
		message="121,insert,paradox";
		CltSocket=sendMessage(ip,port,message,error);
		Thread.sleep(1500);
		port="4167";
		message="*,query";
		CltSocket=sendMessage(ip,port,message,error);
		Thread.sleep(1500);
		message="4,delete";
		CltSocket=sendMessage(ip,port,message,error);
		Thread.sleep(1500);
		port="5054";
		message="*,query";
		CltSocket=sendMessage(ip,port,message,error);
		Thread.sleep(3000);*/
		port="5054";
		message="*,query";
		CltSocket=sendMessage(ip,port,message,error);
		/*try{
			BufferedReader inOne= new BufferedReader(
                new InputStreamReader(CltSocket.getInputStream()));
			String reply=inOne.readLine();
			System.out.println("This is the reply: "+reply);
			CltSocket.close();
		}
		catch(IOException e) {
			e.printStackTrace();
		}
		*/
		return;
	}
}
