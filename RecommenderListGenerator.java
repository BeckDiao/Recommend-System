import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RecommenderListGenerator {
	
	
	public static class RecommenderListGeneratorMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

		/**
		 * Mapper: 
		 * input1: raw input => setup => watchHistory map
		 * input2: Multiplication output: user_id \t movie_id : score => map
		 * 
		 * output: key: user_id    value: movie_id: score
		 */
		
		Map<Integer, List<Integer>> watchHistory = new HashMap<>();
		
		@Override
		protected void setup(Mapper<LongWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			
			Configuration conf = context.getConfiguration();
			String filePath = conf.get("rawInput");
			Path path = new Path(filePath);
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line = br.readLine();
			
			while (line != null) {
				String[] tokens = line.trim().split(",");
				int user_id = Integer.parseInt(tokens[0]);
				int movie_id = Integer.parseInt(tokens[1]);
				if (!watchHistory.containsKey(movie_id)) {
					watchHistory.put(user_id, new ArrayList<Integer>());
					watchHistory.get(user_id).add(movie_id);
				} else {
					watchHistory.get(user_id).add(movie_id);
				}
				line = br.readLine();
			}
			br.close();
		}

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			/**
			 * Map: input value: user_id \t movie_id : score
			 * 		output: key: user_id   value: movieTitles
			 * 
			 */
			
			String[] tokens = value.toString().trim().split("\t");
			int user_id = Integer.parseInt(tokens[0].trim());
			int movie_id = Integer.parseInt(tokens[1].trim());
			
			if (!watchHistory.get(user_id).contains(movie_id)) {
				context.write(new IntWritable(user_id), new Text(movie_id + ":" + tokens[1].trim()));
			}
		}
	}
	
	public static class RecommenderListGeneratorReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

		/**
		 * For Reducer:
		 * input1: <movie_id, movie_titile> => HashMap => setup
		 * input2: mapper output <user_id, mapper_id: score> => reduce
		 * 	
		 * output: <user_id, movieTitle>
		 */
		
		Map<Integer, String> movieTitles = new HashMap<>();
		
		@Override
		protected void setup(Reducer<IntWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			/**
			 * movieTiles file looks like: <1,Dinosaur Planet> -> <Integer, String>
			 */
			
			Configuration conf = context.getConfiguration();
			String filePath = conf.get("movieTilesFile");
			Path path = new Path(filePath);
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line = br.readLine();
			
			while (line != null) {
				String[] tokens = line.trim().split(",");
				int movie_id = Integer.parseInt(tokens[0].trim());
				String movie_title = tokens[1].trim();
				movieTitles.put(movie_id, movie_title);
				line = br.readLine();
			}
			br.close();
		}
		
		
		@Override
		protected void reduce(IntWritable key, Iterable<Text> value,
				Reducer<IntWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
			/**
			 * Reducer: input:  <user_id, movie_id: score> => reduce
			 * 			output: <user_id, movieTitle>
			 * 
			 * Attention: threshold needed for get best k ones instead of showing all movies watched by user
			 */
			
			while (value.iterator().hasNext()) {
				String[] tokens = value.iterator().next().toString().trim().split(":");
				int movie_id = Integer.parseInt(tokens[0].trim());
				double score = Double.parseDouble(tokens[1].trim());
//				if (score < 2) {
//					continue;
//				} else {
//					String title = movieTitles.get(movie_id);
//					context.write(key, new Text(title));
//				}
				String title = movieTitles.get(movie_id);
				context.write(key, new Text(title + ":" + score));
			}
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("rawInput", args[0]);
		conf.set("movieTilesFile", args[1]);
		
		Job job = Job.getInstance(conf);
		job.setMapperClass(RecommenderListGeneratorMapper.class);
		job.setReducerClass(RecommenderListGeneratorReducer.class);
		job.setJarByClass(RecommenderListGenerator.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		TextInputFormat.setInputPaths(job, new Path(args[2]));
		TextOutputFormat.setOutputPath(job, new Path(args[3]));
		
		job.waitForCompletion(true);
	}

}
