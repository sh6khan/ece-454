import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.File;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task4 {
  public static class ParsingMapper extends Mapper<Object, Text, NullWritable, Text> {
    private Text result = new Text();
    private static HashMap<String, int[]> allData = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException{
      Configuration conf = context.getConfiguration();
      URI[] inputURI = Job.getInstance(conf).getCacheFiles();

      for (URI uri : inputURI) {
        Path path = new Path(uri.getPath());
        String filePathName = path.getName();
        BufferedReader reader = new BufferedReader(new FileReader(filePathName));

        String line = "";
        while((line = reader.readLine()) != null) {
          String[] tokens = line.split(",", -1);
          int[] ratings = new int[tokens.length - 1];

          int val = 0;
          for (int i = 1; i < tokens.length; i++) {
            String token = tokens[i];
            val = token.equals("") ? 0 : Integer.valueOf(token);
            ratings[i - 1] = val;
          }

          allData.put(tokens[0], ratings);

        }
      }
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] movieTitle = (String[]) allData.keySet().toArray();
      int[] otherRating;
      int[] rating;
      double cosine;
      String movieName;
      for (int i = 0; i < movieTitle.length; i++) {
        for (int j = i + 1; j < movieTitle.length; j++) {
          otherRating = allData.get(movieTitle[i]);
          rating = allData.get(movieTitle[j]);
          cosine = calculate(rating, otherRating);

          if (movieTitle[i].compareTo(movieTitle[j]) < 0) {
            movieName = movieTitle[i] + "," + movieTitle[j];
          } else {
            movieName = movieTitle[j] + "," + movieTitle[i];
          }

          result.set(movieName + String.format("%1.2f", cosine));
          context.write(NullWritable.get(), result);
        }
      }
    }

    public double calculate(int[] a, int[] b) {

      double mul = 0;
      double aSum = 0;
      double bSum = 0;

      for (int i = 0; i < a.length; i++) {
        mul += a[i] * b[i];
        aSum += a[i] * a[i];
        bSum += b[i] * b[i];
      }

      return (mul / (Math.sqrt(aSum) * Math.sqrt(bSum)));
    }
  }


//  public static class CartesionReducer extends Reducer<Text, ArrayPrimitiveWritable, Text, Text> {
//    private static HashMap<String, int[]> inMemory = new HashMap<>();
//    private static Text moviePair = new Text();
//    private static Text ratingCosine = new Text();
//
//
//    public void reduce(Text key, Iterable<ArrayPrimitiveWritable> values, Context context) throws IOException, InterruptedException {
//      String movieTitle = key.toString();
//      System.out.println(movieTitle);
//
//      int[] ratings = (int[]) values.iterator().next().get();
//
//      StringBuilder sb;
//      for (Map.Entry<String, int[]> entry : inMemory.entrySet()) {
//        sb = new StringBuilder();
//        System.out.println(entry.getKey());
//
//        if (movieTitle.compareTo(entry.getKey()) < 0) {
//          sb.append(movieTitle).append(",").append(entry.getKey());
//        } else {
//          sb.append(entry.getKey()).append(",").append(movieTitle);
//        }
//
//        double res = calculate(ratings, entry.getValue());
//
//        moviePair.set(sb.toString());
//        ratingCosine.set(String.format("%1.2f", res));
//
//        context.write(moviePair, ratingCosine);
//      }
//
//      inMemory.put(movieTitle, ratings);
//    }
//  }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", ",");

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: rating avg <in> <out>");
      System.exit(2);
    }

    Job job = new Job(conf, "cartesion product calc");
    job.setJarByClass(Task4.class);
    job.setMapperClass(Task4.ParsingMapper.class);
    // job.setReducerClass(Task4.CartesionReducer.class);

    // add all the input files into the hadoop distributed cache
    File fp = new File(otherArgs[0]);
    // if its a directory then add all subfiles
    if (fp.isDirectory()) {
      File[] allFiles = fp.listFiles();
      for (File file : allFiles) {
        job.addCacheFile(new Path(file.getAbsolutePath()).toUri());
      }
    } else {
      // else just add this one file
      job.addCacheFile(new Path(otherArgs[0]).toUri());
    }

    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(0);
    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }


}
