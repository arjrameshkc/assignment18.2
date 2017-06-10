# assignment18.2

Create table or recreate table by using below JAVA API


package Hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class CreateTable {
	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		Configuration conf=HBaseConfiguration.create();
		HBaseAdmin admin=new HBaseAdmin(conf);
		HTableDescriptor des=new HTableDescriptor(Bytes.toBytes("employee"));
		des.addFamily(new HColumnDescriptor("details"));
		

		if(admin.tableExists("customer")){
			System.out.println("Table Already exists!");
			admin.disableTable("customer");
			admin.deleteTable("customer");
			System.out.println("Table: employee deleted");
		}
		admin.createTable(des);	
		System.out.println("Table: employee sucessfully created");
	}


}

2. Load the details for this table

package bulkload;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BulkLoadMapReduce {

	static class ImportMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

	    private long ts;

	    static byte[] family = Bytes.toBytes("readings");
	    
	    @Override
	    protected void setup(Context context) {
	    	ts = System.currentTimeMillis();
	    }

		@Override
		public void map(LongWritable offset, Text value, Context context)
				throws IOException {
			try {
				byte[] bRowKey = Bytes.toBytes(value.toString().split("\t")[0]);
				ImmutableBytesWritable rowKey = new ImmutableBytesWritable(bRowKey);
				Put p = new Put(bRowKey);
				p.add(Bytes.toBytes("cf1"), Bytes.toBytes("name"), ts, Bytes.toBytes(value.toString().split("\t")[1]));
				p.add(Bytes.toBytes("cf2"), Bytes.toBytes("exp"), ts, Bytes.toBytes(value.toString().split("\t")[2]));
				context.write(rowKey, p);
				
				// Output of every Mapper will be sorted on rowKey by the framework
				
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	    
		Configuration conf = HBaseConfiguration.create();
		String tableName = "customer";
		Path inputDir = new Path("/user/acadgild/customer.txt");
		Job job = new Job(conf, "bulk_load_mapreduce");
		
		job.setJarByClass(ImportMapper.class);
		FileInputFormat.setInputPaths(job, inputDir);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(ImportMapper.class);

		//Boolean importToTable = true;
		Boolean importToTable = true;
		
		// Run by using: java -cp `hbase classpath`:./bulk-load.jar bulkload.BulkLoadMapReduce
		
		if (importToTable) {
			// Insert into table directly using TableOutputFormat
			// TableOutputFormat takes care of sending Put to the target table
			TableMapReduceUtil.initTableReducerJob(tableName, null, job);
			
			// above will set TableOutputFormat as the output format
			
			job.setNumReduceTasks(0);
		} 
		
		else {
			// Otherwise generate HFile 
			
			HTable table = new HTable(conf, tableName);
			job.setReducerClass(PutSortReducer.class);
			Path outputDir = new Path("/user/acadgild/hbase/bulk_output");
			
			// Row key must not be of zero length
			
			FileOutputFormat.setOutputPath(job, outputDir);
			job.setMapOutputKeyClass(ImmutableBytesWritable.class);
			job.setMapOutputValueClass(Put.class);
			HFileOutputFormat.configureIncrementalLoad(job, table);
			// For every column family, one HFile is generated.
		}		
		
		TableMapReduceUtil.addDependencyJars(job);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}


3. run the java API in HDFS
