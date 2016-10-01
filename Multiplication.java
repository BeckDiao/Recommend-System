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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Multiplication {
	
	public static class MultiplicationMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

		/**
		 * For Mapper: 
		 * input1: raw input<user_id, movie_id, rating>,...
		 * input2: co Matrix<m1: m2, count>,<m1: m3, count>,....
		 * 		=>  HashMap<movie_id, List<MovieRelation>> 
		 * 		=>  HashMap<Integer, Integer> normalize matrix
		 * 
		 * output: 
		 * 		   key: user_id:movie_id
		 * 		   value: score
		 * 
		 */
		
		Map<Integer, List<MovieRelation>> movieRelationMap = new HashMap<>();
		Map<Integer, Integer> denominator = new HashMap<>();
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			/**
			 * 
			 * For setup:
			 * 		co Matrix => HashMap<movie_id, List<MovieRelation>>  => for recording Relation
			 * 				  => HashMap<movie_id, sum> => for recording denominator (normalization)
			 * 
			 */
			
			Configuration conf = context.getConfiguration();
			String filePath = conf.get("CoOccurrencePath");
			Path path = new Path(filePath);
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line = br.readLine();
			
			// line looks like: m1:m2 \t relation
			while (line != null) {
				String[] tokens = line.trim().split("\t");
				String[] movies = tokens[0].trim().split(":");
				
				int movie1 = Integer.parseInt(movies[0]);
				int movie2 = Integer.parseInt(movies[1]);
				int relation = Integer.parseInt(tokens[1]);
				
				
				MovieRelation movieRelation = new MovieRelation(movie1, movie2, relation);
				if (movieRelationMap.containsKey(movie1)) {
					movieRelationMap.get(movie1).add(movieRelation);
				} else {
					movieRelationMap.put(movie1, new ArrayList<MovieRelation>());
					movieRelationMap.get(movie1).add(movieRelation);
				}
				line = br.readLine();
			}
			br.close();
			
			for (Map.Entry<Integer, List<MovieRelation>> entry: movieRelationMap.entrySet()) {
				int sum = 0;
				for (MovieRelation relation: entry.getValue()) {
					sum += relation.getRelation();
				}
				denominator.put(entry.getKey(), sum);
			}	
		}
		
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			/**
			 * now ,we get:
			 * HashMap<movie_id, List<MovieRelation>> movieRelationMap 
			 * HashMap<movie_id, sum> denominator => for normalization
			 * 
			 * Mapper:
			 * 		input: raw input ~ <user_id, movie_id, rating>
			 *		output: <user_id : movie2_id, rating * relation>
			 * 				|-> text                |-> double
			 * 
			 * so, Mapper needs to do :
			 *  1. split input -> movie & rating
			 *  2. get relation set (column in co matrix) from movieRelationMap
			 *  3. get user_id  
			 *  4. multiply rating with relation and write it into context
			 *  
			 */
			
			String[] tokens = value.toString().trim().split(",");
			int rating = Integer.parseInt(tokens[2].trim());
			int movie1 = Integer.parseInt(tokens[1].trim());
			int user_id = Integer.parseInt(tokens[0].trim());
			
			for (MovieRelation relation: movieRelationMap.get(movie1)) {
				double score = rating * relation.getRelation();
				score = score / denominator.get(relation.getMovie2());
				DecimalFormat df = new DecimalFormat("#.00");
				score = Double.valueOf(df.format(score));
				context.write(new Text(user_id + ":" + relation.getMovie2()), new DoubleWritable(score));
			}	
		}		
	}
	
	public static class MultiplicationReducer extends Reducer<Text, DoubleWritable, IntWritable, Text> {

		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> value,
				Reducer<Text, DoubleWritable, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			/**
			 * Reducer:
			 * 		intput: <user_id : movie2_id, score>
			 * 		output: <user_id,  movie_id: score>
			 * 					|-> Integer       |-> Text
			 * 
			 */
			
			String[] tokens = key.toString().trim().split(":");
			int user_id = Integer.parseInt(tokens[0].trim());
			int movie_id = Integer.parseInt(tokens[1].trim());
			
			double score = 0;
			while (value.iterator().hasNext()) {
				score += value.iterator().next().get();
				context.write(new IntWritable(user_id), new Text(movie_id + ":" + score));
			}
		}
	}
	
	

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		conf.set("CoOccurrencePath", args[0]);
		
		Job job = Job.getInstance(conf);
		job.setMapperClass(MultiplicationMapper.class);
		job.setReducerClass(MultiplicationReducer.class);
		job.setJarByClass(Multiplication.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		TextInputFormat.setInputPaths(job, new Path(args[1]));
		TextOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.waitForCompletion(true);

	}

}
