import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task3 {
  public static class RatingAvgMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text word = new Text();
    private final static IntWritable rating = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] tokens = value.toString().split(",", -1);

      for (int i = 1; i < tokens.length; i++) {
        String token = tokens[i];

        if (!token.equals("")) {
          word.set(String.valueOf(i));
          rating.set(Integer.valueOf(tokens[i]));
          context.write(word, rating);
        }
      }

    }
  }

  public static class RatingSumReducer extends Reducer<Text, IntWritable, Text, Text> {
    private final static Text avg = new Text();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      float sum = 0;
      int count = 0;

      for (IntWritable val : values) {
        sum += val.get();
        System.out.println(val.get());
        count += 1;
      }

      sum = sum / count;
      avg.set(String.format("%1.2f", sum));
      context.write(key, avg);
    }
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", ",");

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: rating avg <in> <out>");
      System.exit(2);
    }


    Job job = new Job(conf, "rating avg");
    job.setJarByClass(Task3.class);
    job.setMapperClass(Task3.RatingAvgMapper.class);
    job.setReducerClass(Task3.RatingSumReducer.class);
    //job.setNumReduceTasks(1);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}