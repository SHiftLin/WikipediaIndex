package lsh;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by lsh on 24/12/2017.
 */
public class InvertedIndex {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(InvertedIndex.Map.class);
        job.setReducerClass(InvertedIndex.Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TripleWritable.class);
        job.setMapOutputValueClass(PairLongLongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static String[] slice(char[] text) {
        ArrayList<String> words = new ArrayList<String>();
        int len = text.length;
        String word;
        for (int i = 0, j = 0; i < len; i = j + 1) {
            for (j = i; j < len && !Character.isWhitespace(text[j]); j++) ;
            if (j > i) {
                word = String.valueOf(text, i, j - i);
                if (word.matches("^[A-Za-z]+$")) words.add(word);
            }
        }
        return words.toArray(new String[]{});
    }

    public static class Map extends Mapper<LongWritable, Text, Text, PairLongLongWritable> {

        final static double alpha = 0.2;

        public void map(LongWritable pos, Text value, Context context) throws IOException, InterruptedException {
            long len=value.getLength()+1;
            String word=(value.toString().split(","))[0];
            context.write(new Text(word),new PairLongLongWritable(pos.get(),len));
        }
    }


    public static class Reduce extends Reducer<Text, PairLongLongWritable, Text, TripleWritable> {

        final long INF=((long)1<<30)*((long)1<<30);

        public void reduce(Text word, Iterable<PairLongLongWritable> pairs, Context context) throws IOException, InterruptedException {
            long start=INF,len=0,count=0;
            for(PairLongLongWritable pair:pairs){
                start=Math.min(start,pair.x);
                len+=pair.z;
                count+=1;
            }
            context.write(word,new TripleWritable(start,len,count));
        }

    }
}
