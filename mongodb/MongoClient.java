import java.io.*;
import java.net.*;
import java.text.*;
import java.util.*;

public class MongoClient {

	public static void main(String args[]) throws IOException
	{
		// String serverHostname = new String ("mandar-VirtualBox");
		String serverHostname = new String ("172.31.31.222");

	       // if (args.length > 0)
	         //  serverHostname = args[0];
	        System.out.println ("Attemping to connect to host " +
	                serverHostname + " on port 10008 Indexing Server.");

	        Socket indexSocket = null;
	        PrintWriter out = null;
	        BufferedReader in = null;

	        try {
	            indexSocket = new Socket(serverHostname, 10008);
	            out = new PrintWriter(indexSocket.getOutputStream(), true);
	            in = new BufferedReader(new InputStreamReader(
	                                        indexSocket.getInputStream()));
	        } catch (UnknownHostException e) {
	            System.err.println("Don't know about host: " + serverHostname);
	            System.exit(1);
	        } catch (IOException e) {
	            System.err.println("Couldn't get I/O for "
	                               + "the connection to: " + serverHostname);
	            System.exit(1);
	        }

		//BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
		//String userInput;
		String Location;
		// list of server IPs. String[] ServerIPaddress = {}
		String[] Testfilearr = new String[10001];
		String[] Testpeerarr = new String[10001];
		int[] Portarray = {5050, 5051, 5052, 5053, 5054, 5055, 5056, 5058, 5059, 5060, 5061, 5062, 5063, 5064, 5065, 5066};
		
		for(int i= 1; i<10001; i++)
		{
			Testfilearr[i] = "file"+i+".txt";
		}
		for(int i= 1; i<5001; i++)
		{
			Testpeerarr[i] = "Peer1";
		}
		for(int i= 5001; i<7501; i++)
		{
			Testpeerarr[i] = "Peer2";
		}
		for(int i= 7501; i<10001; i++)
		{
			Testpeerarr[i] = "Peer3";
		}
		int hashvalue;
		int Serverloc;
		int noOfServers = 16;
		SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//dd/MM/yyyy
	    Date now = new Date();
	    String strDate = sdfDate.format(now);
	    System.out.println("Before 10k put"+strDate);
		for(int j=1; j<10001; j++)
		{
		// String[] tokenc = userInput.split(" ");
		 hashvalue = Testfilearr[j].hashCode();
		 Serverloc = Math.abs(hashvalue) % noOfServers;
		 System.out.println("At Server number:" + Serverloc);

		 try 

		 {

		 indexSocket = new Socket(serverHostname, 10008);

		 out = new PrintWriter(indexSocket.getOutputStream(), true);

	 in = new BufferedReader(new InputStreamReader(indexSocket.getInputStream()));

		 } catch (UnknownHostException e)

		 {

		 System.err.println("Don't know about host: " + serverHostname);

		 System.exit(1);

		 } catch (IOException e) 

		 {

		 System.err.println("Couldn't get I/O for " + "the connection to: " + serverHostname);

		 System.exit(1);
		 }
		 out.println("Put");

		 out.println("Put");

		 out.println(Testfilearr[j]);

		 out.println(Testpeerarr[j]);

		 in.readLine();
		 
		 System.out.println("from put"+in.readLine());


		 }
		 Date then = new Date();
		 String strDate1 = sdfDate.format(then);
		 System.out.println("After 10k put"+strDate1);
		 
		 Date now1 = new Date();
		 String strDate2 = sdfDate.format(now1);
		 System.out.println("Before 10k search"+strDate2);
		 
		for(int j=1; j<10001; j++)
		{
		// String[] tokenc = userInput.split(" ");
		 hashvalue = Testfilearr[j].hashCode();
		 Serverloc = Math.abs(hashvalue)% noOfServers;
		 System.out.println("At Server number:" + Serverloc);

		 try 

		 {

		 indexSocket = new Socket(serverHostname, 10008);

		 out = new PrintWriter(indexSocket.getOutputStream(), true);

	 in = new BufferedReader(new InputStreamReader(indexSocket.getInputStream()));

		 } catch (UnknownHostException e)

		 {

		 System.err.println("Don't know about host: " + serverHostname);

		 System.exit(1);

		 } catch (IOException e) 

		 {

		 System.err.println("Couldn't get I/O for " + "the connection to: " + serverHostname);

		 System.exit(1);
		 }
		out.println("Search");

		out.println("Search");

		out.println(Testfilearr[j]);

		System.out.println("from search"+in.readLine());


		}
		
		 Date then1 = new Date();
		 String strDate3 = sdfDate.format(then1);
		 System.out.println("After 10k search"+strDate3);
		 
		 Date now2 = new Date();
		 String strDate4 = sdfDate.format(now2);
		 System.out.println("Before 10k delete"+strDate4);
		 
		for(int j=1; j<10001; j++)
		{
		// String[] tokenc = userInput.split(" ");
		 hashvalue = Testfilearr[j].hashCode();
		 Serverloc = Math.abs(hashvalue) % noOfServers;
		 System.out.println("At Server number:" + Serverloc);

		 try 

		 {

		 indexSocket = new Socket(serverHostname, 10008);

		 out = new PrintWriter(indexSocket.getOutputStream(), true);

	 in = new BufferedReader(new InputStreamReader(indexSocket.getInputStream()));

		 } catch (UnknownHostException e)

		 {

		 System.err.println("Don't know about host: " + serverHostname);

		 System.exit(1);

		 } catch (IOException e) 

		 {

		 System.err.println("Couldn't get I/O for " + "the connection to: " + serverHostname);

		 System.exit(1);
		 }

		out.println("Del");

		out.println("Del");

		out.println(Testfilearr[j]);

		System.out.println("from del"+in.readLine());

		}	
		
		Date then2 = new Date();
		 String strDate5 = sdfDate.format(then2);
		 System.out.println("After 10k search"+strDate5);
		out.close();

		in.close();

		//stdIn.close();

		indexSocket.close();	

	}
}
