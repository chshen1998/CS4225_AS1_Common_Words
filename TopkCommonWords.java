/*
NAME: CHEW HOA SHEN
MATRICULATION NUMBER: A0200044E
*/

import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Set;
import java.util.Map;
import java.util.StringTokenizer;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopkCommonWords {

    private static Set<String> stopWords = new HashSet<String>();

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private HashMap<String, Integer> wordCount; 

        public void setup(Context context) throws IOException, InterruptedException {
          wordCount = new HashMap<>();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
          StringTokenizer itr = new StringTokenizer(value.toString());
          while (itr.hasMoreTokens()) {
            String curr = itr.nextToken();

            if (!stopWords.contains(curr)) {
              if (wordCount.containsKey(curr)) {
                wordCount.put(curr, wordCount.get(curr) + 1);
              } else {
                wordCount.put(curr, 1);
              }
            }
          }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
          for (Map.Entry<String, Integer> entry : wordCount.entrySet()) {
              context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));            
          }
      }
    }

    public static class IntMinReducer extends Reducer<Text, IntWritable, IntWritable, Text> {

        private HashMap<Integer, List<String>> minCounts;

        public void setup(Context context) throws IOException, InterruptedException {
          minCounts = new HashMap<>();
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
          int counter = 0;
          int minVal = Integer.MAX_VALUE;

          for (IntWritable val : values) {
            if (val.get() < minVal) {
                minVal = val.get();
            } 
            counter += 1;
          }

          if (counter == 2) {
            List<String> wordList = minCounts.get(minVal);
            if (wordList == null) {
              wordList = new ArrayList<String>();
              wordList.add(key.toString());
              minCounts.put(minVal, wordList);
            } else {
              wordList.add(key.toString());
            }
          }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
          int count = 0;

          minCounts.entrySet().stream()
          .sorted(Map.Entry.<Integer, List<String>>comparingByKey())
          .forEach(entry -> {
            try {
              List<String> values = entry.getValue();
              Collections.sort((values));
              for (int i =0; i < values.size() && count < 10; i++) {
                context.write(new IntWritable(entry.getKey()), new Text(values.get(i)));
                count ++;
              }              
            } catch (IOException | InterruptedException e) {
              System.out.println("Error in Reducer cleanup");
              e.printStackTrace();
            }
          });
        }  
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Scanner scanner = new Scanner(new File(args[2]));
        while (scanner.hasNextLine()) {
          String stopWord = scanner.nextLine();
          stopWords.add(stopWord);
        }

        Job job = Job.getInstance(conf, "Top K Common Words");
        job.setJarByClass(TopkCommonWords.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(IntMinReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    } 
}
