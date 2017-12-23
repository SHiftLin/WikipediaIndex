package lsh;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by lsh on 20/12/2017.
 */

public class PairLongLongWritable implements Writable {

    public long x;
    //public double y;
    public long z;
    //public double s = 0;

    public PairLongLongWritable() {
        super();
    }

    public PairLongLongWritable(long _x, long _z) {
        x = _x;
        //y = _y;
        z = _z;
        //s = (1 - p) * y + p * z * 1000;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(x);
        //out.writeDouble(y);
        out.writeLong(z);
        //out.writeDouble(s);
    }

    public void readFields(DataInput in) throws IOException {
        x = in.readLong();
        //y = in.readDouble();
        z = in.readLong();
        //s = in.readDouble();
    }

    @Override
    public String toString() {
        return String.valueOf(x) + "," + String.valueOf(z);
    }
}
