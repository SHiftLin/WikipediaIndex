package lsh;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by lsh on 20/12/2017.
 */

public class TupleWritable implements Writable {

    final double p = 0.2;
    //public long x;
    public double y;
    public long z;
    public double s = 0;

    public TupleWritable() {
        super();
    }

    public TupleWritable(double _y, long _z) {
        //x = _x;
        y = _y;
        z = _z;
        s = (1 - p) * y + p * z * 1000;
    }

    public void write(DataOutput out) throws IOException {
        //out.writeLong(x);
        out.writeDouble(y);
        out.writeLong(z);
        out.writeDouble(s);
    }

    public void readFields(DataInput in) throws IOException {
        //x = in.readLong();
        y = in.readDouble();
        z = in.readLong();
        s = in.readDouble();
    }

    @Override
    public String toString() {
        return /*String.valueOf(x) + "," + */ String.format("%.2f", y) + "," + String.valueOf(z) + "," + String.format("%.2f", s);
    }
}
