import org.bson.BasicBSONObject;

import com.mongodb.*;

import java.net.*;
import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.lang.Object;
import com.mongodb.WriteConcern;

public class MongoServer extends Thread{
	
	 protected Socket clientSocket;
	 public static void main(String[] args) throws Exception {
	        ServerSocket serverSocket = null; 

	        try {
	        	try { 
	                serverSocket = new ServerSocket(10008); 
	                System.out.println ("Connection Socket Created");
	                try { 
	                     while (true)
	                        {
	                         System.out.println ("Waiting for Connection");
	                         new MongoServer (serverSocket.accept()); 
	                        }
	                    } 
	                catch (IOException e) 
	                    { 
	                     System.err.println("Accept failed."); 
	                     System.exit(1); 
	                    } 
	               } 
	           catch (IOException e) 
	               { 
	                System.err.println("Could not listen on port: 10008."); 
	                System.exit(1); 
	               } 
	        }
	           finally
	               {
	                try {
	                     serverSocket.close(); 
	                    }
	                catch (IOException e)
	                    { 
	                     System.err.println("Could not close port: 10008."); 
	                     System.exit(1); 
	                    } 
	               }
	        
	        }
	        	private MongoServer (Socket clientSoc)
	            {
	             clientSocket = clientSoc;
	             start();
	            }
	          public void run()
	          {
	           System.out.println ("New Communication Thread Started");
	           try {

	          	 PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true); 
	          	 BufferedReader in = new BufferedReader(new InputStreamReader( clientSocket.getInputStream())); 
	          	
	              String keyfromclient = null;
	              String locfromclient = null;
              	//  String[] host = {};

              	 /*Mongo Database initialization ends*/
            	    
                	
	              while(in.readLine() != null)
	              {
	            	  
	            	  MongoClient mongoClient = new MongoClient("localhost",27017);

		              DB db = mongoClient.getDB("AnuradhaDB");
		              
	            	  //DBCollection collection = db.createCollection( "AOS_Final_Project,", null ); 

		             DBCollection collection = db.getCollection("AOS_Final_Project");	 
		              BasicDBObject registry = new BasicDBObject("title", "AOS_Final_project");
		              mongoClient.setWriteConcern(WriteConcern.NORMAL);
		              
	              if(in.readLine().equals("Put"))
	              {  
	            	 
		              
	              keyfromclient = in.readLine();
	              locfromclient = in.readLine();
	              Object loc = locfromclient;
	              registry.append(keyfromclient, loc);
	              collection.insert(registry);
	              }
	              if(in.readLine().equals("Get"))
	              {
	              keyfromclient = in.readLine();
	              Object object = registry.get(keyfromclient);
	              out.println("Result of get: ");
	              }
	              if(in.readLine().equals("Del"))
	              {
	              keyfromclient = in.readLine();
	              Object remove = registry.remove(keyfromclient);
	              out.println("Result of del: ");
	              }
	              }
	              out.close(); 
	              in.close(); 
	              clientSocket.close();
	           } 
	           catch (IOException e) 
	               { 
	                System.err.println("Problem with Communication Server");
	                System.exit(1); 
	               } catch (InterruptedException e) {
	  			// TODO Auto-generated catch block
	  			e.printStackTrace();
	  		} 
	           }
}
