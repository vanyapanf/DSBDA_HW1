package bdtc.lab1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Редьюсер: суммирует все единицы полученные от {@link HW1Mapper}, выдаёт суммарное количество пользователей по браузерам
 */
public class HW1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private Map<String, Integer> min_temperature_values = new HashMap<String, Integer>() {{
        put("low", 0);
        put("medium", 1000);
        put("high", 10000);
    }};

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        while (values.iterator().hasNext()) {
            sum += values.iterator().next().get();
            System.out.println("key:"+key+" value:"+sum);
        }
        context.write(key, new IntWritable(sum));
        System.out.println("key:"+key+" value:"+sum+" temperature:"+getTemperatureValue(sum));
    }

    protected String getTemperatureValue(int val) {
        String temperature = "";
        for (Map.Entry<String, Integer> entry: min_temperature_values.entrySet()) {
            Integer value = entry.getValue();
            if (val >= value)
                temperature = entry.getKey();
        }
        return temperature;
    }
}
