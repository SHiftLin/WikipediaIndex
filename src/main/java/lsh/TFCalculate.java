package lsh;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class TFCalculate {
    private static HashSet stopwords = new HashSet<String>();

    public static void main(String[] args) throws Exception {
        byte[] buffer = new byte[1024 * 1024];
        ClassLoader classLoader = TFCalculate.class.getClassLoader();
        InputStream fin = classLoader.getResourceAsStream("stopwords");
        int len = fin.read(buffer);
        String[] words = new String(buffer, 0, len).split("\n");
        for (String word : words) stopwords.add(word);
        System.out.println(stopwords.size());

        Configuration conf = new Configuration();
        conf.set("xmlinput.start", "<page>");
        conf.set("xmlinput.end", "</page>");

        Job job = Job.getInstance(conf);
        job.setJarByClass(TFCalculate.class);
        job.setMapperClass(TFCalculate.Map.class);

        job.setInputFormatClass(XmlInputFormat.class);
        job.setOutputKeyClass(PairStringDoubleWritable.class);
        job.setOutputValueClass(PairLongLongWritable.class);

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

    public static class Map extends Mapper<LongWritable, Text, PairStringDoubleWritable, PairLongLongWritable> {

        final static double alpha = 0.2;

        public void map(LongWritable pos, Text value, Context context) throws IOException, InterruptedException {
            HashMap<String, Long> count = new HashMap<String, Long>();

            String text = value.toString();

            WikiClean cleaner = new WikiClean.Builder().withTitle(false).build();
            Long id = Long.parseLong(cleaner.getId(text));
            String title = cleaner.getTitle(text).toLowerCase();
            String content = cleaner.clean(text).toLowerCase();

            String[] words = slice(title.toCharArray());
            int m = words.length;
            for (int i = 0; i < m; i++)
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
                double tf = 1.0 * entry.getValue() / (n + m);
                int z = (title.indexOf(key) == -1) ? 0 : 1;
                double s = ((1 - alpha) * tf + alpha * z) * 1000.0;
                context.write(new PairStringDoubleWritable(key, s), new PairLongLongWritable(id, z));
            }
        }
    }

 /*   public static class Combiner extends Reducer<Text, PairLongLongWritable, Text, TupleArrayWritable> {

        public void reduce(Text word, Iterable<PairLongLongWritable> docs, Context context) throws IOException, InterruptedException {
            for(PairLongLongWritable doc:docs)
            {
                doc.
            }
            context.write(Text,Iterable);
        }

    }*/

   /* public static class Reduce extends Reducer<Text, PairLongLongWritable, Text, TupleArrayWritable> {

        public void reduce(Text word, Iterable<PairLongLongWritable> docs, Context context) throws IOException, InterruptedException {
            ArrayList<PairLongLongWritable> array = new ArrayList<PairLongLongWritable>();
            for (PairLongLongWritable doc : docs) {
                PairLongLongWritable d=new PairLongLongWritable(doc.x, doc.y, doc.z);
                array.add(d);
            }
        *//*    Comparator<PairLongLongWritable> cmp = new Comparator<PairLongLongWritable>() {
                public int compare(PairLongLongWritable a, PairLongLongWritable b) {
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

            context.write(word, new TupleArrayWritable(array.toArray(new PairLongLongWritable[]{})));
        }

    }*/
}
