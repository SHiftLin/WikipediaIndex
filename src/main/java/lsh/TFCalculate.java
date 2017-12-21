package lsh;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.wikiclean.WikiClean;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by lsh on 13/12/2017.
 */

class TupleArrayWritable extends ArrayWritable {
    public TupleArrayWritable() {
        super(TupleArrayWritable.class);
    }

    public TupleArrayWritable(TupleWritable[] arrays) {
        super(TupleWritable.class);
        set(arrays);
    }

    @Override
    public String toString() {
        String[] strs = toStrings();
        String res = "";
        for (int i = 0; i < strs.length; i++) {
            res += strs[i];
            if (i + 1 < strs.length) res += "\t";
        }
        return res;
    }
}

public class TFCalculate {
    private static HashSet stopwords = new HashSet<String>();

    public static void main(String[] args) throws Exception {
        byte[] buffer = new byte[1024 * 1024];
        ClassLoader classLoader = TFCalculate.class.getClassLoader();
        InputStream fin = classLoader.getResourceAsStream("stopwords");
        int len = fin.read(buffer);
        String[] words = new String(buffer, 0, len).split("\n");
        for (String word : words) stopwords.add(word);

        Configuration conf = new Configuration();
        conf.set("xmlinput.start", "<page>");
        conf.set("xmlinput.end", "</page>");

        Job job = Job.getInstance(conf);
        job.setJarByClass(TFCalculate.class);
        job.setMapperClass(TFCalculate.Map.class);
        // job.setCombinerClass(IntSumReducer.class);
        // job.setReducerClass(TFCalculate.Reduce.class);

        job.setInputFormatClass(XmlInputFormat.class);
        //job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(TupleArrayWritable.class);
        //job.setMapOutputValueClass(TupleWritable.class);
        job.setOutputKeyClass(PairWritable.class);
        job.setOutputValueClass(TupleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static String[] slice(char[] text) {
        ArrayList<String> words = new ArrayList<String>();
        int len = text.length;
        for (int i = 0, j = 0; i < len; i = j + 1) {
            for (j = i; j < len && Character.isLetter(text[j]); j++) ;
            if (j > i) words.add(String.valueOf(text, i, j - i));
        }
        return words.toArray(new String[]{});
    }

    public static class Map extends Mapper<LongWritable, Text, PairWritable, TupleWritable> {

        public void map(LongWritable pos, Text value, Context context) throws IOException, InterruptedException {
            HashMap<String, Long> count = new HashMap<String, Long>();

            String text = value.toString();

            WikiClean cleaner = new WikiClean.Builder().withTitle(false).build();
            Long id = Long.parseLong(cleaner.getId(text));
            String title = cleaner.getTitle(text).toLowerCase();
            String content = cleaner.clean(text).toLowerCase();

            String[] words = slice(title.toCharArray());
            for (int i = 0; i < words.length; i++)
                count.put(words[i], new Long(0));

            words = slice(content.toCharArray());
            int n = words.length;
            for (int i = 0; i < n; i++) {
                String word = words[i];
                if (stopwords.contains(word)) continue;
                if (!count.containsKey(word)) count.put(word, new Long(0));
                count.put(word, count.get(word) + 1);
            }

            for (HashMap.Entry<String, Long> entry : count.entrySet()) {
                String key = entry.getKey();
                double tf = 1000.0 * entry.getValue() / n;
                context.write(new PairWritable(key, id), new TupleWritable(tf, (title.indexOf(key) == -1) ? 0 : 1));
            }
        }
    }

 /*   public static class Combiner extends Reducer<Text, TupleWritable, Text, TupleArrayWritable> {

        public void reduce(Text word, Iterable<TupleWritable> docs, Context context) throws IOException, InterruptedException {
            for(TupleWritable doc:docs)
            {
                doc.
            }
            context.write(Text,Iterable);
        }

    }*/

   /* public static class Reduce extends Reducer<Text, TupleWritable, Text, TupleArrayWritable> {

        public void reduce(Text word, Iterable<TupleWritable> docs, Context context) throws IOException, InterruptedException {
            ArrayList<TupleWritable> array = new ArrayList<TupleWritable>();
            for (TupleWritable doc : docs) {
                TupleWritable d=new TupleWritable(doc.x, doc.y, doc.z);
                array.add(d);
            }
        *//*    Comparator<TupleWritable> cmp = new Comparator<TupleWritable>() {
                public int compare(TupleWritable a, TupleWritable b) {
                    Double A=new Double(a.s);
                    Double B=new Double(b.s);
                    return A.compareTo(B);
                    *//**//*if(a.s<b.s) return -1;
                    else if(a.s>b.s) return 1;
                    else return 0;*//**//*
                }
            };
            System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
            Collections.sort(array, cmp);*//*

            context.write(word, new TupleArrayWritable(array.toArray(new TupleWritable[]{})));
        }

    }*/
}
