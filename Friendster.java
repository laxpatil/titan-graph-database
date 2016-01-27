import java.io.File;
import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.Scanner;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;

import org.apache.tinkerpop.gremlin.*;
/**
 * 
 */

/**
 * @author laxmikant
 *  Loads the dataset of Friedster into Cassandra and extracts Using Titan
 *  
 *  Sample Dataset *************
 *  99:notfound
   	100:notfound
	101:private
	102:101,181,794,798,804,811,814,821,996,6319,6533,40495,148648,861441,967502,1064745,1077747,2806491,9229534,17838679
	103:notfound
	104:101,1143,628701,2438054
 *  Sample Dataset *************
 *  
 */
public class Friendster {

	/**
	 * @param args
	 * @throws FileNotFoundException 
	 */
	

    public static void load(final TitanGraph graph,String filePath) throws FileNotFoundException {
    	Scanner sc = new Scanner(new File(filePath));
		System.out.println("Inside Load Function");
    	 TitanTransaction tx = graph.newTransaction();
    	
		for (int i =0 ; sc.hasNext();i++)
		{
		    String friendLine = sc.next();
		    
		    String friendList[]= friendLine.split(":");
		    if(friendList.length==1)
		    {
		    	continue;
		    }
		    else if(friendList[1].equals("notfound"))
		    {
		    	String human="human";
		    	tx.addVertex(T.label, human, "Name", "Not Found","No of Friends",0);
		    	
		    	// tx.commit();
		    }
		    else if(friendList[1].equals("private"))
		    {
		    	String human="human";
		    	tx.addVertex(T.label, human, "Name", ""+friendList[0],"No of Freinds", "Private");
		    	System.out.println("Node Added : "+ friendList[0]);
		    	
		    	// tx.commit();
		    }
		    else
		    {
		    	String human="human";
		    	int friends_count=friendList[1].split(",").length;
		    	
		    	tx.addVertex(T.label, human, "Name", ""+friendList[0],"No of Friends",friends_count);
		    	System.out.println("Node Added : "+ friendList[0]);
		    	String totalList[]=friendList[1].split(",");
		    	
		    	for(int j=0;j<totalList.length;j++)
		    	{
		    		 Iterator<Vertex> itr2=graph.traversal().V().has("Name", ""+totalList[j]);
		    	      if(!itr2.hasNext())
		    	      {
		    	    	  tx.addVertex(T.label, human, "Name", ""+totalList[j],"No of Friends",999);
		    	    	  System.out.println("Node Added : "+ totalList[j]);
		    	    	  
		    	    	  //	 tx.commit();
		    	      }
		    	}
		    }
		    
		}
		
		tx.commit();
		
    	
           }

    /**
     * Calls {@link TitanFactory#open(String)}, passing the Titan configuration file path
     * which must be the sole element in the {@code args} array, then calls
     * {@link #load(com.thinkaurelius.titan.core.TitanGraph)} on the opened graph,
     * then calls {@link com.thinkaurelius.titan.core.TitanGraph#close()}
     * and returns.
     * <p/>
     * This method may call {@link System#exit(int)} if it encounters an error, such as
     * failure to parse its arguments.  Only use this method when executing main from
     * a command line.  Use one of the other methods on this class ({@link #create(String)}
     * or {@link #load(com.thinkaurelius.titan.core.TitanGraph)}) when calling from
     * an enclosing application.
     *
     * @param args a singleton array containing a path to a Titan config properties file
     * @throws FileNotFoundException 
     */
    
	
	public static void main(String[] args) throws FileNotFoundException {
		// TODO Auto-generated method stub
		
		//for (String line : Files.readAllLines(Paths.get("/path/to/file.txt"))) {
		    // ...
	//	}
		
		 TitanGraph g = TitanFactory.open("titan-cassandra.properties");
		 
	//	load(g,"/media/laxmikant/New Volume/friends.txt");
		
		 // to check the nodes
		 
		 Iterator<Vertex> itr= g.vertices("Name","150");
	       if(itr.hasNext())
	       {
	    	   
	    	   System.out.println("Not Empty");
	    	   
	       }
	       else
	    	   System.out.println("Empty");
	    	   
	      while(itr.hasNext())
	      {
	    	 Vertex v= itr.next();
	    	
	    	// System.out.println("Vertex :  " + v.property("name").value().equals("pluto"));
	      }
		 
	      Iterator<Vertex> itr2=g.traversal().V().has("Name", "125");
	      if(!itr2.hasNext())
	      {
	    	  System.out.println("Null");
	      }
	      else
	      System.out.println("Vertex by API : "+itr2.next().value("No of Friends"));
	      
	      g.close();
		 
	}

}
