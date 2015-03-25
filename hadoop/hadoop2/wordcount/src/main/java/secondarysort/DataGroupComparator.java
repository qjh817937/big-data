package secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DataGroupComparator extends WritableComparator {

    protected DataGroupComparator() {
        super(StringPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        StringPair p1 = (StringPair) a;
        StringPair p2 = (StringPair) b;
        String s1 = p1.getFirst();
        String s2 = p2.getFirst();
        return s1.compareTo(s2);
    }

}
