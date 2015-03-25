package secondarysort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StringPair  implements WritableComparable<StringPair> {
    /**
     * id
     */
    private String first;
    /**
     * timestamp
     */
    private Long second;

    public StringPair() {
        this("", 0L);
    }

    public StringPair(String first, Long second) {
        this.first = first;
        this.second = second;
    }

    public void set(String first, Long second) {
        this.first = first;
        this.second = second;
    }

    public String getFirst() {
        return first;
    }

    public Long getSecond() {
        return second;
    }

    //@Override
    public void readFields(DataInput in) throws IOException {
        first = in.readUTF();
        second = in.readLong();
    }

    //@Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(first);
        out.writeLong(second);
    }

    //@Override
    public int compareTo(StringPair o) {
        if (!first.equals(o.getFirst()))
            return first.compareTo(o.getFirst());
        else if (!second.equals(o.getSecond()))  {
            return o.getSecond().compareTo(second);
        }
        else
            return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null)
            return false;
        if (this == o)
            return true;
        if (o instanceof StringPair) {
            StringPair pair = (StringPair) o;
            return first == pair.getFirst() && second == pair.getSecond();
        } else
            return false;
    }
}
