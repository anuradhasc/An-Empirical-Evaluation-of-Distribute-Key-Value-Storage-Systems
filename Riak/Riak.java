import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class Riak {
// A basic POJO class to demonstrate typed exchanges with Riak
	public static class Book {
		public String title;
		public String author;
		public String body;
		public String isbn;
		public Integer copiesOwned;
	}

	// This will create a client object that we can use to interact with Riak
	private static RiakCluster setUpCluster(String s) throws UnknownHostException {
		// This example will use only one node listening on localhost:10017
		RiakNode node = new RiakNode.Builder()
				.withRemoteAddress(s)
				.withRemotePort(8087)
				.build();

		// This cluster object takes our one node as an argument
		RiakCluster cluster = new RiakCluster.Builder(node)
				.build();

		// The cluster must be started to work, otherwise you will see errors
		cluster.start();

		return cluster;
	}

	public static void main( String[] args ) 
	{
		try 
		{
			BufferedReader fr = new BufferedReader(new FileReader("D:/Anuradha/AOS/PA3/config.txt"));
			String s = null;
			s = fr.readLine();
			Namespace quotesBucket = new Namespace("RiakTable");
			
			System.out.println("Location object created for quote object");

			// With our RiakObject in hand, we can create a StoreValue operation
			
			RiakCluster cluster = setUpCluster(s);
			RiakClient client = new RiakClient(cluster);
			System.out.println("Client object successfully created");

			String key, value;
			int random = new Random().nextInt(10)+1;
			int min = (random)*100000-100000;
			int max = (random)*100000-1;
			
			SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//dd/MM/yyyy
		    Date now = new Date();
		    String strDate = sdfDate.format(now);
		    System.out.println("Before 10k put"+strDate);
			Location quoteObjectLocation = null;
			RiakObject quoteObject = new RiakObject().setContentType("text/plain");
			StoreValue storeOp=null;
			
			for(int i=min ; i<=max ; i++)
			{
				// First, we'll create a basic object storing a movie quote
				key = String.format("%10s", String.valueOf(i)).replace(" ", "*");
				value = String.format("%90s", "randomStringForKey-"+i+"-").replace(" ", "*");

				quoteObjectLocation = new Location(quotesBucket, key);
				
				quoteObject.setValue(BinaryValue.create(value));
				
				storeOp = new StoreValue.Builder(quoteObject)
						.withLocation(quoteObjectLocation)
						.build();
				//System.out.println("StoreValue operation created");

				StoreValue.Response storeOpResp = client.execute(storeOp);
				//System.out.println("Object storage operation successfully completed");
				
			}
			Date then = new Date();
			String strDate1 = sdfDate.format(then);
			System.out.println("After 10k put"+strDate1);
			 
			Date now1 = new Date();
			String strDate2 = sdfDate.format(now1);
			System.out.println("Before 10k search"+strDate2);

			min = (random)*100000-100000;
			max = (random)*100000-1;
						
			for(int i=min;i<=max;i++)
			{
				FetchValue fetchOp = new FetchValue.Builder(quoteObjectLocation).build();
				RiakObject fetchedObject = client.execute(fetchOp).getValue(RiakObject.class);
				assert(fetchedObject.getValue().equals(quoteObject.getValue()));
				//System.out.println("Success! The object we created and the object we fetched have the same value");
			}
			

			min = (random)*100000-100000;
			max = (random)*100000-1;
			
			Date then1 = new Date();
			String strDate3 = sdfDate.format(then1);
			System.out.println("After 10k search"+strDate3);
			 
			Date now2 = new Date();
			String strDate4 = sdfDate.format(now2);
			System.out.println("Before 10k delete"+strDate4);

			for(int i=min;i<=max;i++)
			{
				// And we'll delete the object
				DeleteValue deleteOp = new DeleteValue.Builder(quoteObjectLocation).build();
				client.execute(deleteOp);
				System.out.println("Quote object successfully deleted");
			}
			Date then2 = new Date();
			String strDate5 = sdfDate.format(then2);
			System.out.println("After 10k search"+strDate5);


			cluster.shutdown();

		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
}