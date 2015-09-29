import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.util.Bytes;

public class SuperTable{

   public static void main(String[] args) throws IOException {

      // Instantiate Configuration class
	  Configuration  config = HBaseConfiguration.create();
		
      // Instaniate HBaseAdmin class
	  HBaseAdmin admin = new HBaseAdmin(config);
	  
      // Instantiate table descriptor class
	  HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("mp4"));

      // Add column families to table descriptor
	  tableDescriptor.addFamily(new HColumnDescriptor("personal"));
	  tableDescriptor.addFamily(new HColumnDescriptor("professional"));

      // Execute the table through admin
	  admin.createTable(tableDescriptor);
	  System.out.println("################ Table created... ################");

      // Instantiating HTable class
	  HTable hTable = new HTable(config, "powers");
     
      // Repeat these steps as many times as necessary

	      // Instantiating Put class
              // Hint: Accepts a row name
	  Put p1 = new Put(Bytes.toBytes("row1"));
	  Put p2 = new Put(Bytes.toBytes("row2"));
	  Put p3 = new Put(Bytes.toBytes("row3"));

      	      // Add values using add() method
              // Hints: Accepts column family name, qualifier/row name ,value
	  p1.add(Bytes.toBytes("personal"), Bytes.toBytes("hero"), Bytes.toBytes("superman"));
	  p1.add(Bytes.toBytes("personal"), Bytes.toBytes("power"), Bytes.toBytes("strength"));
	  
	  p1.add(Bytes.toBytes("professional"), Bytes.toBytes("name"), Bytes.toBytes("clark"));
	  p1.add(Bytes.toBytes("professional"), Bytes.toBytes("xp"), Bytes.toBytes("100"));
	  
	  p2.add(Bytes.toBytes("personal"), Bytes.toBytes("hero"), Bytes.toBytes("batman"));
	  p2.add(Bytes.toBytes("personal"), Bytes.toBytes("power"), Bytes.toBytes("money"));
	  
	  p2.add(Bytes.toBytes("professional"), Bytes.toBytes("name"), Bytes.toBytes("bruce"));
	  p2.add(Bytes.toBytes("professional"), Bytes.toBytes("xp"), Bytes.toBytes("50"));
	  
	  p3.add(Bytes.toBytes("personal"), Bytes.toBytes("hero"), Bytes.toBytes("wolverine"));
	  p3.add(Bytes.toBytes("personal"), Bytes.toBytes("power"), Bytes.toBytes("healing"));
	  
	  p3.add(Bytes.toBytes("professional"), Bytes.toBytes("name"), Bytes.toBytes("logan"));
	  p3.add(Bytes.toBytes("professional"), Bytes.toBytes("xp"), Bytes.toBytes("75"));
	  
      // Save the table
	  hTable.put(p1);
	  hTable.put(p2);
	  hTable.put(p3);
	
      // Close table
	  hTable.close();
	  
      // Instantiate the Scan class
	  Scan scan = new Scan();
     
      // Scan the required columns
	  scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("hero"));

      // Get the scan result
	  HTable table = new HTable(config, "powers");
	  ResultScanner scanner = table.getScanner(scan);

      // Read values from scan result
      // Print scan result
	  for (Result result = scanner.next(); result != null; result = scanner.next())
		  System.out.println(result);
 
      // Close the scanner
	  scanner.close();
   
      // Htable closer
   }
}

