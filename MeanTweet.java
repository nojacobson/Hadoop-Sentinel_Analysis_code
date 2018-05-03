import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.json.*;
import java.math.*;
import java.awt.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.Object;
import java.io.*;
import java.util.HashMap;
import java.net.*;



/*
    Noah Jacobson
    nj367715@ohio.edu
    THIS IS SOLUTION 3

*/


public class MeanTweet {

    public static class TMap extends Mapper<Object, Text, Text, IntWritable>{
        
        // a hashmap that contains a word with a "weight" to it
        private static HashMap<String, Integer> hashMap = new HashMap<String, Integer>();
       //private static HashMap<String, Integer> userMap = new HashMap<String, Integer>();

        // This loads the hashmap with a given list
        public void setup(Context context){

            try {
                // retrieves the name of the file
                URI[] dictionary = context.getCacheFiles(); // returns a URI object
                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path dictPath = new Path(dictionary[0].toString()); // the path to the textfile in hdfs
                // a reader that is opened to the path of the given dictionary
                BufferedReader read = new BufferedReader(new InputStreamReader(fs.open(dictPath)));

                String [] word_num; // array that holds word and value of that word
                Integer value; // holder for the integer value of that number

                String line = read.readLine();
                
                // Go thru file reading in words and values until the end is reached
                while(line != null){    
                    //Split the line of mean word and value
                    word_num = line.split("\\t");
                    word_num[0] = word_num[0].trim();
                    //Process the line and get the integer trimmed of whitespace
                    value = Integer.parseInt(word_num[1].trim());

                    //System.out.println(word_num[0] + ": " + word_num[1]);

                    //Put the word and value into the hashmap
                    hashMap.put(word_num[0], value);
                    line = read.readLine();         
                }

                read.close();

            } catch (IOException e) {
                System.err.println("Error reading dictionary file.");
            }
        }

        public void map(Object key, Text value, Context context)throws IOException, InterruptedException{
            // creates a JSON object from the value Map is given
            JSONObject jsn = new JSONObject(value.toString());
            JSONObject userObj; // tweet object
            String text;   // text in the tweet
            String name; // the ID of the tweet

            // retrieves the tweet and id 
            try {
                text = (String) jsn.get("text");
            } catch (JSONException je) {
                text = (String) "missing tweet";
            }
            try {
                userObj = jsn.getJSONObject("user");
                try {
                    name = (String) userObj.get("screen_name");
                } catch (JSONException je2) {
                    name = (String) "Missing user???";
                }
            } catch (JSONException je1) {
                name = "missinng user";
            }
         
            // removing all punctuation and converting to lowercase
            String [] textarr = text.replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+"); 
            Integer array_length = textarr.length;
            Integer rating;
            Integer tweet_total = 0;

            // going thru text looking for "mean" words
            for(int i = 0; i < array_length; i++){
                rating = hashMap.get(textarr[i]);

                if(rating != null && rating < 0) {
                    tweet_total += (-1 * rating);
                }
            }

            Text username = new Text(name);
            IntWritable ratingwrite = new IntWritable(tweet_total);
            // writing to context the tweet ID and rating
            //System.out.println("Name: " + name + " \t" + tweet_total);
            context.write(username, ratingwrite);
        }
    }
    public static class TReduce extends Reducer<Text,IntWritable,Text,IntWritable> {
        
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
          int sum = 0;

          for (IntWritable val : values) {
            //System.out.println(val.get());
            sum += val.get();
          }

          result.set(sum);
          context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 3) {
          System.err.println("Usage: Main <dictionary-file> <input> <output>");
          System.exit(2);
        }

        Job job = Job.getInstance(conf, "MeanTweet");
        job.addCacheFile(new URI(args[0])); // dictionay file
        job.setJarByClass(MeanTweet.class); // class of the jar
        job.setMapperClass(TMap.class);     // class of mapper
        job.setCombinerClass(TReduce.class);// class of combiner, one was not creaated
        job.setReducerClass(TReduce.class); // class of reducer
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[1])); // file that contains the tweets
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2])); // dir that will be writen to
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}