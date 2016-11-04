package com.Hadoop;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.gson.Gson;

public class HBaseSample {

	public static void main(String[] args) {

		/*
		 * Create the configuraiton
		 */
		Configuration conf = HBaseConfiguration.create();

		/*
		 * The name of the table that will be created
		 */
		TableName tableName = TableName.valueOf("Social-Network");

		try {          
			/*
			 * Read user input
			 */
			Scanner reader = new Scanner(System.in);

			/*
			 * Create the connection
			 */
			Connection conn = ConnectionFactory.createConnection(conf);

			/*
			 * Get the admin of the connection
			 */
			Admin hAdmin = conn.getAdmin();

			/*
			 * Put which is considered as a row in the table
			 */
			Put p;
			String fName, lName, eMail, bffName, friend, friendsList;
			List<String> friends;

			/*
			 * Instantiate the descriptor of the columns
			 */
			HTableDescriptor hTableDesc = new HTableDescriptor(tableName);
			hTableDesc.addFamily(new HColumnDescriptor("info"));
			hTableDesc.addFamily(new HColumnDescriptor("friends"));

			/*
			 * Create the table if not already exists
			 */
			if (!hAdmin.tableExists(tableName))
				hAdmin.createTable(hTableDesc);

			/*
			 * Once the table created, we get it to add new rows
			 */
			Table table = conn.getTable(tableName);

			System.out.println("Press enter to add a new user. Enter \"quit\" once you finished adding users.");

			/*
			 * The user must press Enter to continue
			 */
			while (!reader.nextLine().equals("")){
				System.out.println("You shall press enter to pass!");
			}

			/*
			 * The user can add a new user while he doesn't type "quit"
			 */
			do {
				// we get the first name
				System.out.println("Enter your first name: ");
				fName = reader.nextLine();

				// then, the last name
				System.out.println("Enter your last name: ");
				lName = reader.nextLine();

				// then, the email address
				System.out.println("Enter your e-mail address: ");
				while (!(eMail = reader.nextLine()).contains("@")){
					System.out.println("This is not a valid e-mail address. Enter your e-mail address:");
				}

				// then the best friend forever
				System.out.println("Enter your best friend's name: ");
				bffName = reader.nextLine();

				// and lastly the list of all the friends
				System.out.println("Enter all your friends' names. Press Enter when you have no more. Next friend's name?");
				friends = new ArrayList<String>();
				
				// once the user presses enter, it means there is no more friends
				while (!(friend=reader.nextLine()).equals("")){
					friends.add(friend);
					System.out.println("Next friend's name?");
				}
				// we transform the list of string to json format
				friendsList = new Gson().toJson(friends);

				// the id of a row is the first name of the user
				p = new Put(Bytes.toBytes(fName));
				
				// we add the other information as columns
				p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("lastName"), Bytes.toBytes(lName));
				p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("eMail"), Bytes.toBytes(eMail));
				p.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("bffName"), Bytes.toBytes(bffName));
				p.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("friends"), Bytes.toBytes(friendsList));

				// then we insert the row entirely into the table
				table.put(p);
			}while(!"quit".equals(reader.nextLine()));
			
			// we close the reader to free memory
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}