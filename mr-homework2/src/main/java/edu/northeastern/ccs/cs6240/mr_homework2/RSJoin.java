package edu.northeastern.ccs.cs6240.mr_homework2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

class FlaggedEdges{
	String from;
	String to;
	char flag;
	public FlaggedEdges(String from, String to, char flag){
		this.from = from;
		this.to = to;
		this.flag = flag;
		}
	public char getFlag() {
		return this.flag;
	}
	public String getFromNode() {
		return this.from;
	}
	public String getToNode() {
		return this.to;
	}
}

class Triangle{
	String node1;
	String node2;
	String node3;
	public Triangle(String n1,String n2,String n3) {
		this.node1=n1;
		this.node2=n2;
		this.node3=n3;
	}
}
class Edge{
	String from;
	String to;
	public Edge(String from,String to) {
		this.from = from;
		this.to= to;
	}
	
}

public class RSJoin extends Configured implements Tool{
	private static final Logger logger = LogManager.getLogger(RSJoin.class);
	
	public static class RSMapper extends Mapper<Object, Text, Text, FlaggedEdges> {
		private final Text from = new Text();
		private final Text to = new Text();
		private final Text val = new Text();
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			String line = value.toString();
		    String[] user_follower = line.split(",");
		    	from.set(user_follower[0]);
		    	to.set(user_follower[1]);
		    	FlaggedEdges from_val = new FlaggedEdges(user_follower[0],user_follower[1],'S');
		    	FlaggedEdges to_val = new FlaggedEdges(user_follower[0],user_follower[1],'T');
		    	context.write(from,from_val);
		    	context.write(to, to_val);
		    	
		    }
	}
	public static class RSReducer extends Reducer<Text, FlaggedEdges, Text, Triangle> {
		private final IntWritable result = new IntWritable();

		@Override
		public void reduce(final Text key, final Iterable<FlaggedEdges> values, final Context context) throws IOException, InterruptedException {
			List<Edge> SList = new ArrayList<>();
			List<Edge> TList = new ArrayList<>();
			
			for (final FlaggedEdges val : values) {
				if(val.getFlag()=='S') {
					Edge e = new Edge(val.getFromNode(),val.getToNode());
					SList.add(e);
				}
				if(val.getFlag()=='T') {
					Edge e = new Edge(val.getFromNode(),val.getToNode());
					TList.add(e);
				}
			}
			
		}
	}



	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

}
