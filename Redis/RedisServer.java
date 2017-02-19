import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

public class RedisServer {

	public static void main(String[] args) {
		Properties config = new ServerProperties().loadProperties();
		Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
	//Jedis Cluster will attempt to discover cluster nodes automatically
		for(int i=1 ; i<=Integer.parseInt(config.getProperty("numServer")) ; i++)
		{
		  jedisClusterNodes.add(new HostAndPort(config.getProperty("Server"+i), 8000));
		}
	    JedisCluster jcluster = new JedisCluster(jedisClusterNodes);
	    
	    	String key, value;
			int random = new Random().nextInt(10)+1;
			int min = (random)*100000-100000;
			int max = (random)*100000-1;
		
			SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//dd/MM/yyyy
		    Date now = new Date();
		    String strDate = sdfDate.format(now);
		    System.out.println("Before 100k put"+strDate);
			for(int i=min ; i<=max ; i++){
				key = String.format("%10s", String.valueOf(i)).replace(" ", "*");
				value = String.format("%90s", "randomStringForKey-"+i+"-").replace(" ", "*");
				jcluster.set(key, value);
			}
			 Date then = new Date();
			 String strDate1 = sdfDate.format(then);
			 System.out.println("After 100k put"+strDate1);
			 
			 Date now1 = new Date();
			 String strDate2 = sdfDate.format(now1);
			 System.out.println("Before 100k search"+strDate2);

			
				 random = new Random().nextInt(10)+1;
				 min = (random)*100000-100000;
				 max = (random)*100000-1;
			
				for(int i=min ; i<=max ; i++){
					key = String.format("%10s", String.valueOf(i)).replace(" ", "*");
					value = String.format("%90s", "randomStringForKey-"+i+"-").replace(" ", "*");
					jcluster.put(key, value);
				}
				Date then1 = new Date();
				 String strDate3 = sdfDate.format(then1);
				 System.out.println("After 100k search"+strDate3);
				 
				 Date now2 = new Date();
				 String strDate4 = sdfDate.format(now2);
				 System.out.println("Before 100k delete"+strDate4);
				
			     
				 random = new Random().nextInt(10)+1;
				 min = (random)*100000-100000;
				 max = (random)*100000-1;
				
				for(int i=min ; i<=max ; i++){
					key = String.format("%10s", String.valueOf(i)).replace(" ", "*");
					jcluster.del(key);
				}
				 
				Date then2 = new Date();
				 String strDate5 = sdfDate.format(then2);
				 System.out.println("After 100k delete"+strDate5);

}
}