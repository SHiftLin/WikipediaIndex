package lsh;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by lsh on 21/12/2017.
 */
public class PairWritable implements Writable {

    public String x;
    public long y;

    public PairWritable() {
        super();
    }

    public PairWritable(String _x, long _y) {
        x = _x;
        y = _y;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(x);
        out.writeLong(y);
    }

    public void readFields(DataInput in) throws IOException {
        x = in.readUTF();
        y = in.readLong();
    }

    @Override
    public String toString() {
        return x + "," + String.valueOf(y);
    }

}
