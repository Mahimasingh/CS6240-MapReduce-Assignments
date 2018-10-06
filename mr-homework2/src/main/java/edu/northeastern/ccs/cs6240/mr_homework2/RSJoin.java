package edu.northeastern.ccs.cs6240.mr_homework2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/*
 * This class is used to represent the output of Two-length mapper in the form (from,to,flag)
 */
class FlaggedEdges implements Writable{
	String from;
	String to;
	String flag;
	public FlaggedEdges(String from, String to, String flag){
		this.from = from;
		this.to = to;
		this.flag = flag;
		}
	public FlaggedEdges() {
		// TODO Auto-generated constructor stub
	}
	public String getFlag() {
		return this.flag;
	}
	public String getFromNode() {
		return this.from;
	}
	public String getToNode() {
		return this.to;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeChars(from);
		out.writeChars(to);
		out.writeChars(flag);
		
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		String line = in.readLine();
		from = line.split(",")[0].substring(1, line.split(",")[0].length() -1);
	    to = line.split(",")[1].substring(1, line.split(",")[1].length() -1);
	    flag = Character.toString(line.split(",")[2].charAt(1));
		
	}
}

/*
 * This class represents an edge
 */
class Edge{
	String from;
	String to;
	public Edge(String from,String to) {
		this.from = from;
		this.to= to;
	}
	public Edge() {
		// TODO Auto-generated constructor stub
	}
	public String getFrom() {
		return this.from;
	}
	public String getTo() {
		return this.to;
	}
	
	
}
/*
 * This class produces 2-edge paths.
 */
public class RSJoin extends Configured implements Tool{
	private static final Logger logger = LogManager.getLogger(RSJoin.class);
	public static int count = 0;
	
	public static class RSMapper extends Mapper<Object, Text, Text, FlaggedEdges> {
		private final Text from = new Text();
		private final Text to = new Text();
		FlaggedEdges from_val = new FlaggedEdges();
		FlaggedEdges to_val = new FlaggedEdges();
		
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			//logger.info("ALL IS FINE mapper");
			String line = value.toString();
		    String[] user_follower = line.split(",");
		    if(Integer.parseInt(user_follower[1]) < 1000 && Integer.parseInt(user_follower[0]) < 1000)
		    	from.set(user_follower[0]);
		    	to.set(user_follower[1]);
		    	from_val = new FlaggedEdges(user_follower[0]+",",user_follower[1]+",","S");
		    	to_val = new FlaggedEdges(user_follower[0]+",",user_follower[1]+",","T");
		    	context.write(from,from_val);
		    	context.write(to, to_val);
		    	
		    }
	}
	public static class RSReducer extends Reducer<Text, FlaggedEdges, Text, Text> {
		private Text from = new Text();
		private Text to = new Text();
		Edge e1 = new Edge();
		Edge e2 = new Edge();

		@Override
		public void reduce(final Text key, final Iterable<FlaggedEdges> values, final Context context) throws IOException, InterruptedException {
		
			List<Edge> SList = new ArrayList<>();
			List<Edge> TList = new ArrayList<>();
			for (FlaggedEdges val : values) {
				String flag = val.getFlag();
				if(flag.compareTo("T") == 0) {
					e1 = new Edge(val.getFromNode(),val.getToNode());
					SList.add(e1);
				}
				
				else if(flag.compareTo("S") == 0) {
					e2 = new Edge(val.getFromNode(),val.getToNode());
					TList.add(e2);
				}
				else
					logger.info("Something fishy happened!!");
			}
			count = count + SList.size() * TList.size();
			
			for(int i =0; i< SList.size();i++) {
				for(int j = 0; j < TList.size(); j++) {
					if(!(SList.get(i).getFrom().equals(TList.get(j).getTo()) && SList.get(i).getTo().equals(TList.get(j).getFrom()))){
						from.set(SList.get(i).getFrom());
						to.set(TList.get(j).getTo());
						context.write(from,to);
					}
					}
					
			}
			System.out.println("COUNT IS " + count);
			
		}
	}



	public int run(String[] args) throws Exception {
		final Configuration conf = getConf();
	    final Job job = Job.getInstance(conf, "Two Paths");
		job.setJarByClass(RSJoin.class);
		final Configuration jobConf = job.getConfiguration();
		job.setMapperClass(RSMapper.class);
		job.setReducerClass(RSReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlaggedEdges.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true)?0:1;
		
	}
	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new RSJoin(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}
		
}


