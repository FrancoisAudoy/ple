package bigdata;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
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
import org.apache.spark.api.java.JavaPairRDD;

@SuppressWarnings("serial")
public final class ConnectionToHBase implements Serializable{

	private final byte[] Table_Name = Bytes.toBytes("TilesAF");
	private final byte[] Table_Name_Test = Bytes.toBytes("TilesAFTest");
	private final byte[] PositionFamilyName = Bytes.toBytes("Position");
	private final byte[] DataFamilyName = Bytes.toBytes("Tile");
	private final int MaxVersion = 10;

	private StringUtils strUtil;

	public ConnectionToHBase() {
		strUtil = new StringUtils();
	}

	public void createTable(Connection connection) throws IOException{
		final Admin admin = connection.getAdmin();

		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(Table_Name_Test));
		HColumnDescriptor positionFamily = new HColumnDescriptor(PositionFamilyName);
		HColumnDescriptor dataFamily = new HColumnDescriptor(DataFamilyName);

		positionFamily.setMaxVersions(MaxVersion);
		dataFamily.setMaxVersions(MaxVersion);

		tableDescriptor.addFamily(positionFamily);
		tableDescriptor.addFamily(dataFamily);

		createOrOverwrite(admin, tableDescriptor);
		admin.close();

	}

	public Connection connectTable() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(conf);

		return connection;
	}

	public void SavesTiles(JavaPairRDD<String, byte[]> rddToSave) throws IOException {

		rddToSave.foreachPartition(tiles ->{

			Connection connection = connectTable();
			Table table = connection.getTable(TableName.valueOf(Table_Name));

			ArrayList<Put> allPut = new ArrayList<>();

			tiles.forEachRemaining(tile -> {
				Put put = new Put(Bytes.toBytes(tile._1.toUpperCase()));

				String [] posParsed = strUtil.extractLonLat(tile._1);
				try {
					if(posParsed[0] != null && posParsed[1] != null && posParsed[2] != null) {

						put.addColumn(PositionFamilyName, ("Lon").getBytes(), posParsed[0].getBytes());
						put.addColumn(PositionFamilyName, ("Lat").getBytes(), posParsed[1].getBytes());
						put.addColumn(PositionFamilyName, ("Z").getBytes(), posParsed[2].getBytes());
						put.addColumn(DataFamilyName, ("img").getBytes(), tile._2);

						allPut.add(put);
					}

				}
				catch(ArrayIndexOutOfBoundsException e) {
					throw new RuntimeException(posParsed.length + "");
				}
			});

			table.put(allPut);

		});

	}

	private void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
		if (admin.tableExists(table.getTableName())) {
			admin.disableTable(table.getTableName());
			admin.deleteTable(table.getTableName());
		}
		admin.createTable(table);
	}
}
