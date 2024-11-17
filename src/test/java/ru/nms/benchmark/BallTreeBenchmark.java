package ru.nms.benchmark;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import ru.nms.KnnService;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static ru.nms.TestUtils.constructSourceFilePath;

@Fork(1)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 200, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 3, time = 200, timeUnit = TimeUnit.MILLISECONDS)
public class BallTreeBenchmark {

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void testBallTreeKnn(ExecutionPlan plan, Blackhole blackhole) throws IOException {
        blackhole.consume(KnnService.knn(plan.getTarget(), plan.getK(), constructSourceFilePath(plan.getN(), plan.getDimension())));
    }

}
