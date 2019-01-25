package bigdata;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.PropertiesConfiguration.PropertiesReader;
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
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;

import scala.Tuple2;

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

		rddToSave.mapToPair(tile ->{

				Put put = new Put(Bytes.toBytes(tile._1));

				String [] posParsed = strUtil.extractLonLat(tile._1);

				put.addColumn(PositionFamilyName, ("Lon").getBytes(), posParsed[0].getBytes());
				put.addColumn(PositionFamilyName, ("Lat").getBytes(), posParsed[1].getBytes());
				put.addColumn(PositionFamilyName, ("Z").getBytes(), posParsed[2].getBytes());
				put.addColumn(DataFamilyName, ("img").getBytes(), tile._2);

				return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);

		}).saveAsNewAPIHadoopDataset(getHBaseConf());

	}

	private void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
		if (admin.tableExists(table.getTableName())) {
			admin.disableTable(table.getTableName());
			admin.deleteTable(table.getTableName());
		}
		admin.createTable(table);
	}

	private Configuration getHBaseConf() {
		Configuration conf =  HBaseConfiguration.create();
		
		conf.set("hbase.zookeeper.qourum", "young:9000");
		conf.set("hbase.mapred.outputtable", "TilesAFTest");
		conf.set("mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat");
		conf.set("mapreduce.job.key.class", "org.apache.hadoop.hbase.io.ImmutableBytesWritable");
		conf.set("mapreduce.job.output.value.class", "org.apache.hadoop.hbase.util.Bytes");
		conf.set("mapreduce.output.fileoutputformat.outputdir", "tmp");
		conf.setBoolean("hbase.cluster.distributed", true);
		return conf;
	}

//	private void putData(String lonLat, byte[] data) throws IllegalArgumentException, IOException{
//		Connection connection = connectTable();
//		Table table = connection.getTable(TableName.valueOf(Table_Name));
//
//		Put put = new Put(Bytes.toBytes(lonLat));
//
//		String [] posParsed = strUtil.extractLonLat(lonLat);
//
//		put.addColumn(PositionFamilyName, ("Lon").getBytes(), posParsed[0].getBytes());
//		put.addColumn(PositionFamilyName, ("Lat").getBytes(), posParsed[1].getBytes());
//		put.addColumn(PositionFamilyName, ("Z").getBytes(), "1".getBytes());
//		put.addColumn(DataFamilyName, ("img").getBytes(), data);
//
//		table.put(put);
//
//		//return put;
//
//	}
}
