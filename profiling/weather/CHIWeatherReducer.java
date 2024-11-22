import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.FloatWritable;
import java.util.List;
import java.util.ArrayList;

public class CHIWeatherReducer
    extends Reducer<Text, FloatWritable, Text, Text> {

    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {
        
        List<Float> valueList = new ArrayList<>();
        for (FloatWritable value : values) {
            valueList.add(value.get());
        }

        StringBuilder outputStr = new StringBuilder();
        getMax(valueList, outputStr);
        getMin(valueList, outputStr);
        getAvg(valueList, outputStr);
        getStd(valueList, outputStr);
        context.write(key, new Text(outputStr.toString()));
    }

    private void getMax(List<Float> values, StringBuilder outputStr) {
        float maxValue = Float.MIN_VALUE;
        for (Float value : values) {
            maxValue = Math.max(maxValue, value);
        }
        outputStr.append("max:");
        outputStr.append(maxValue);
        outputStr.append(" ");
    }
    private void getMin(List<Float> values, StringBuilder outputStr) {
        float minValue = Float.MAX_VALUE;
        for (Float value : values) {
            minValue = Math.min(minValue, value);
        }
        outputStr.append("min:");
        outputStr.append(minValue);
        outputStr.append(" ");
    }
    private void getAvg(List<Float> values, StringBuilder outputStr) {
        int totalCount = values.size();
        float totalSum = 0;
        for (Float value : values) {
            totalSum += value;
        }
        float mean = totalSum / totalCount;
        float varianceSum = 0;
        for (Float value : values) {
            float diff = value - mean;
            varianceSum += diff * diff;
        }
        float stdDev = (float) Math.sqrt(varianceSum / totalCount);
        outputStr.append("std:");
        outputStr.append(stdDev);
        outputStr.append(" ");
    }
}

