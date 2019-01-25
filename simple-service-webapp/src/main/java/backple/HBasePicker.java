package backple;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.util.Tool;

public class HBasePicker extends Configured implements Tool{
	
	private static final TableName TABLENAME=TableName.valueOf("TilesAF");
	private static Connection connection;
	private static Table table;
	
	public static byte[] get(String row){
		Get g=new Get(row.getBytes());
		Result r;
		try {
			r=table.get(g);
			return r.getValue("Tile".getBytes(), "img".getBytes());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
		
	}
	

	@Override
	public int run(String[] arg0) throws Exception {
		if(connection==null){
			connection = ConnectionFactory.createConnection(getConf());
		}
		if(table==null){
			table=connection.getTable(TABLENAME);
		}
		return 0;
	}

}
