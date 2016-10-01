import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class DataDivideByUser {
	
	public class DataDivideMapper extends Mapper<IntWritable, Text, IntWritable, Text> {

		@Override
		protected void map(IntWritable key, Text value, Mapper<IntWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			// input value: 1,10001,3.0 <user_id, moive_id, rating>
			// wanted output: key: <user_id>, value: <"movie_id:rating">
			// so, mapper is to split
			
			String[] user_movie_rating = value.toString().trim().split(","); // ["1", "10001", "3.0"]			
			int userID = Integer.parseInt(user_movie_rating[0]);
			StringBuilder sb = new StringBuilder();
			sb.append(user_movie_rating[1]);
			sb.append(":");
			sb.append(user_movie_rating[2]);
			
			context.write(new IntWritable(userID), new Text(sb.toString().trim()));
		}	
	}
	
	public class DatatDivideReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

		@Override
		protected void reduce(IntWritable key, Iterable<Text> value, Reducer<IntWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			// Reducer input: key: user_id, value: Iterator<"movie_id:rating">
			// Reducer output: key: <user_id>   value: "movie_id1:rating", "movie_id2,:rating", ...
			// so, Reducer is to group by user_id, change interator into one string
			
			StringBuilder sb = new StringBuilder();
			while (value.iterator().hasNext()) {
				sb.append(",");
				sb.append(value.iterator().next());
			}
			context.write(key, new Text(sb.toString().replaceFirst(",", "")));
			
		}
		
	}
	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setMapperClass(DataDivideMapper.class);
		job.setReducerClass(DatatDivideReducer.class);
		job.setJarByClass(DataDivideByUser.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}

}
