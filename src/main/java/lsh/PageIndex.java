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

/**
 * Created by lsh on 13/12/2017.
 */

class LongArrayWritable extends ArrayWritable {
    public LongArrayWritable() {
        super(LongWritable.class);
    }

    public LongArrayWritable(LongWritable[] arrays) {
        super(LongWritable.class);
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

public class PageIndex {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("xmlinput.start", "<page>");
        conf.set("xmlinput.end", "</page>");

        Job job = Job.getInstance(conf);
        job.setJarByClass(PageIndex.class);
        job.setMapperClass(PageIndex.Map.class);

        job.setInputFormatClass(XmlInputFormat.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongArrayWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class Map extends Mapper<LongWritable, Text, LongWritable, LongArrayWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String text = value.toString();
            WikiClean cleaner = new WikiClean.Builder().withTitle(true).build();
            LongWritable id = new LongWritable(Long.parseLong(cleaner.getId(text)));
            LongWritable len = new LongWritable(value.getLength());
            if (id.get() == -1) return;
            context.write(id, new LongArrayWritable(new LongWritable[]{key, len}));
        }

    }

}
