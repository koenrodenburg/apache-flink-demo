import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Round {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream = StreamUtil.getDataStream(env, args);

        DataStream<Long> roundedDataStream = dataStream
                .filter((FilterFunction<String>) s -> {
                    try{
                        Double.parseDouble(s.trim()); // No use for the value, just verifying that no exception is thrown
                        return true;
                    } catch (Exception e) {
                        return false;
                    }
                })
                .map((MapFunction<String, Long>) s -> {
                    double d = Double.parseDouble(s.trim());
                    return Math.round(d);
                });

        roundedDataStream.print();

        env.execute("Round");
    }
}
