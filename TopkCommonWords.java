/*
NAME: CHEW HOA SHEN
MATRICULATION NUMBER: A0200044E
*/

import java.io.IOException;
import java.util.Arrays;
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

import javafx.util.Pair;

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

    public static class IntMinReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        private List<Pair<Integer, Text>> minCounts;

        public void setup(Context context) throws IOException, InterruptedException {
          minCounts = new TreeMap<>();
        }

        public void reduce(org.w3c.dom.Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
          int counter = 0;
          int minVal = Integer.MAX_VALUE;

          for (IntWritable val : values) {
            if (val.get() < minVal) {
                minVal = val.get();
            } 
            counter += 1;
          }

          if (counter == 2) {
            minCounts.add(new Pair<Integer, Text>(smallest, new Text(key.toString())));
          }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
          Collections.sort(minCounts, new Comparator<Pair<Integer, Text>>() {
            public int compare(Pair<Integer, Text> word1, ImmutablePair<Integer, Text> word2) {
                return word1.getLeft() - word2.getLeft();
            }
        });

        for (int i = 0; i < 10 && i < minCounts.size(); i++) {
            context.write(new IntWritable(minCounts.get(i).getLeft()), minCounts.get(i).getRight());
        }
        }  
    }

    public static void main(String[] args){
        Configuration conf = new Configuration();

        Scanner scanner = new Scanner(new File(args[1]));
        while (scanner.hasNextLine()) {
          String stopWord = scanner.nextLine();
          stopWords.add(stopWord);
        }

        Job job = Job.getInstance(conf, "Top K Common Words");
        job.setJarByClass(TopkCommonWords.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntMinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    } 
}
