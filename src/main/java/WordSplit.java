import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordSplit {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream = StreamUtil.getDataStream(env, args);
        DataStream<String> wordDataStream = dataStream
                .flatMap((FlatMapFunction<String, String>) (sentence, collector) -> {
                    for(String word : sentence.split(" ")) {
                        collector.collect(word);
                    }
                })
                .returns(new TypeHint<String>(){}.getTypeInfo());

        wordDataStream.print();

        env.execute("WordSplit");
    }
}
