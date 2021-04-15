package bdtc.lab1;

import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class HW1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Map<String, int[]> screen_areas = new HashMap<String, int[]>() {{
        put("top-left", new int[] {0, 426, 682, 1024});
        put("mid-left", new int[] {0, 426, 341, 682});
        put("bot-left", new int[] {0, 426, 0, 341});
        put("top-mid", new int[] {426, 852, 682, 1024});
        put("mid-mid", new int[] {426, 852, 341, 682});
        put("bot-mid", new int[] {426, 852, 0, 341});
        put("top-right", new int[] {852, 1280, 682, 1024});
        put("mid-right", new int[] {852, 1280, 341, 682});
        put("bot-right", new int[] {852, 1280, 0, 341});
    }};

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (Pattern.matches("^(\\d+) (\\d+) (\\d+) (\\d+)$", line)) {
            String[] line_parts = line.split(" ");
            int x = Integer.parseInt(line_parts[0]);
            int y = Integer.parseInt(line_parts[1]);
            if (getScreenArea(x,y) != null) {
                word.set(getScreenArea(x,y));
                context.write(word, one);
            }
            else {
                context.getCounter(CounterType.MALFORMED).increment(1);
            }
        } else {
            context.getCounter(CounterType.MALFORMED).increment(1);
        }
    }

    protected String getScreenArea(int x, int y) {
        for (Map.Entry<String,int[]> entry: screen_areas.entrySet()) {
            int[] value = entry.getValue();
            if (x >= value[0] && x <= value[1] && y >= value[2] && y <= value[3])
                return entry.getKey();
        }
        return null;
    }


}
