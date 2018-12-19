package bigdata;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public final class ConnectionToHBase implements Serializable{

	public static final byte[] Table_Name = Bytes.toBytes("TilesAF");
	public static final byte[] PositionFamilyName = Bytes.toBytes("Position");
	public static final byte[] DataFamilyName = Bytes.toBytes("Tile");
	
	public static final int MaxVersion = 10;
	
	private ConnectionToHBase() {}
	
	public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
		if (admin.tableExists(table.getTableName())) {
			admin.disableTable(table.getTableName());
			admin.deleteTable(table.getTableName());
		}
		admin.createTable(table);
	}

	public static void createTable(Connection connection) throws IOException{
		final Admin admin = connection.getAdmin();

		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(Table_Name));
		HColumnDescriptor positionFamily = new HColumnDescriptor(PositionFamilyName);
		HColumnDescriptor dataFamily = new HColumnDescriptor(DataFamilyName);
		
		positionFamily.setMaxVersions(MaxVersion);
		dataFamily.setMaxVersions(MaxVersion);

		tableDescriptor.addFamily(positionFamily);
		tableDescriptor.addFamily(dataFamily);

		createOrOverwrite(admin, tableDescriptor);
		admin.close();

	}

	public static Connection connectTable() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(conf);
		return connection;
	}

	public static void putData(Connection connection, String lonLat, byte[] data) throws IllegalArgumentException, IOException {
		HTable table = (HTable) connection.getTable(TableName.valueOf(Table_Name));
		/*
		saveAsNewAPIHadoopDataset
		 */
		Put put = new Put(Bytes.toBytes(lonLat));

		String [] lonLatparsed = StringUtils.extractLonLat(lonLat);

		put.addColumn(PositionFamilyName, ("lon").getBytes(), lonLatparsed[0].getBytes());
		put.addColumn(PositionFamilyName, ("lat").getBytes(), lonLatparsed[1].getBytes());
		put.addColumn(PositionFamilyName, ("img").getBytes(), data);
		
		table.put(put);
		table.flushCommits();
		
	}
}
