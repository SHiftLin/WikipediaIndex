package lsh;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by lsh on 21/12/2017.
 */
public class PairStringDoubleWritable implements WritableComparable<PairStringDoubleWritable> {

    public String x;
    public double y;

    public PairStringDoubleWritable() {
        super();
    }

    public PairStringDoubleWritable(String _x, double _y) {
        x = _x;
        y = _y;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(x);
        out.writeDouble(y);
    }

    public void readFields(DataInput in) throws IOException {
        x = in.readUTF();
        y = in.readDouble();
    }

    public int compareTo(PairStringDoubleWritable op) {
        int cmp = x.compareTo(op.x);
        if (cmp != 0) return cmp;
        return new Double(y).compareTo(op.y);
    }

    @Override
    public String toString() {
        return x + "," + String.format("%.2f", y);
    }

}
