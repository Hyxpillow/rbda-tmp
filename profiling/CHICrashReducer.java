import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class CHICrashReducer
    extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

        StringBuilder outputStr = new StringBuilder();
        getNull(values, outputStr);
        
        if (key.get() == "CRASH_YEAR") {
            getMax(values, outputStr);
            getMin(values, outputStr);
        } else if (key.get() == "POSTED_SPEED_LIMIT") {
            getMax(values, outputStr);
            getMin(values, outputStr);
            getAvg(values, outputStr);
        } else (key.get() == "INJURIES_TOTAL") {
            getSum();
        }
        context.write(key, new Text(outputStr.toString()));
    }

    private void getNull(Iterable<Text> values, StringBuilder outputStr) {
        int totalCount = 0;
        int nullCount = 0;
        for (Text value : values) {
            totalCount += 1;
            if (value.get() == "") {
                nullCount += 1;
            }
        }
        outputStr.append("nullCount:");
        outputStr.append(nullCount);
        outputStr.append(" ");
    }

    private void getMax(Iterable<Text> values, StringBuilder outputStr) {
        int maxValue = Integer.MIN_VALUE;
        for (Text value : values) {
            if (value.get() != "") {
                int year = Integer.parseInt(value.get());
                maxValue = Math.max(maxValue, year);
            }
        }
        outputStr.append("max:");
        outputStr.append(maxValue);
        outputStr.append(" ");
        
    }

    private void getMin(Iterable<Text> values, StringBuilder outputStr) {
        int minValue = Integer.MAX_VALUE;
        for (Text value : values) {
            if (value.get() != "") {
                int year = Integer.parseInt(value.get());
                minValue = Math.min(minValue, year);
            }
        }
        outputStr.append("min:");
        outputStr.append(minValue);
        outputStr.append(" ");
    }

    private void getAvg(Iterable<Text> values, StringBuilder outputStr) {
        int totalCount = 0;
        long totalSum = 0;
        for (Text value : values) {
            if (value.get() != "") {
                totalSum += Integer.parseInt(value.get());
                totalCount += 1;
            }
        }
        outputStr.append("average:");
        outputStr.append(totalSum / totalCount);
        outputStr.append(" ");
    }

    private void getSum(Iterable<Text> values, StringBuilder outputStr) {
        long totalSum = 0;
        for (Text value : values) {
            if (value.get() != "") {
                totalSum += Integer.parseInt(value.get());
            }
        }
        outputStr.append("sum:");
        outputStr.append(totalSum);
        outputStr.append(" ");
    }

}

