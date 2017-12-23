package lsh;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by lsh on 24/12/2017.
 */
public class TripleWritable {
    public long x;
    public long y;
    public long z;

    public TripleWritable() {
        super();
    }

    public TripleWritable(long _x, long _y, long _z) {
        x = _x;
        y = _y;
        z = _z;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(x);
        out.writeLong(y);
        out.writeLong(z);
    }

    public void readFields(DataInput in) throws IOException {
        x = in.readLong();
        y = in.readLong();
        z = in.readLong();
    }

    @Override
    public String toString() {
        return String.valueOf(x) + "," + String.valueOf(y) + "," + String.valueOf(z);
    }
}
