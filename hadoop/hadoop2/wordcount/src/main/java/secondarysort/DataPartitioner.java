package secondarysort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class DataPartitioner extends Partitioner<StringPair, Text> {

    @Override
    public int getPartition(StringPair key, Text val, int numPartitions) {
        String strKey = key.getFirst();
        try {
            long lKey = Long.parseLong(strKey);
            return (int)(lKey % numPartitions);
        }catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

}
