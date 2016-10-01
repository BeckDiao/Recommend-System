import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class CoOccurrenceMatrixGenerator {
	
	/**
	 * 
	 * this MR is to get co-occurrence matrix
	 * in the other word, we need to get all movie pairs for every user
	 * 
	 * 
	 * @author xindiao
	 *
	 */
	
	public static class MatrixGeneratorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			//input: user_id \t movie1:rating, movie2:rating, movie3:rating....
			//wanted output: key: movide1:movie2     value: 1
			// so, mapper is to split
			
			String[] user_movieRating = value.toString().trim().split("\t");
			String[] movie_rating = user_movieRating[0].trim().split(",");
			
			for (int i = 0; i < movie_rating.length; i++) {
				String movie1 = movie_rating[i].trim().split(":")[0];
				for (int j = 0; j < movie_rating.length; j++) {
					String movie2 = movie_rating[j].trim().split(":")[1];
					context.write(new Text(movie1 + ":" + movie2), new IntWritable(1));
				}
			}
		}
	}
	
	
	public static class MatrixGeneratorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> value,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) 
						throws IOException, InterruptedException {
			int sum = 0;
			while (value.iterator().hasNext()) {
				sum += value.iterator().next().get();
			}
			context.write(key, new IntWritable(sum));
		}
		
	}
	
	
	

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setMapperClass(MatrixGeneratorMapper.class);
		job.setReducerClass(MatrixGeneratorReducer.class);
		job.setJarByClass(CoOccurrenceMatrixGenerator.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}

}
