package ru.nms.benchmark;

import org.apache.commons.math3.linear.RealVector;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import ru.nms.KnnService;
import ru.nms.TestDataGenerator;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static ru.nms.TestUtils.constructSourceFilePath;

@Fork(1)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 200, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 20, time = 200, timeUnit = TimeUnit.MILLISECONDS)
public class SearchBenchmark {

    @Param({"_midPoint", "_sample"})
    private String fileType;

    private int seed = 0;
    private Random random;
    private RealVector target;
    private final int dimension = 256;
    private final int k = 100;
    private final int n = 1000000;
    private final int leafSize = 1000;

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }

    @Setup(Level.Iteration)
    public void setupIteration() {
        random = new Random(++seed);
    }

    @Setup(Level.Trial)
    public void setupTrial() {
        seed = 0;
        random = new Random(seed);
        target = TestDataGenerator.generateNVectors(random, 1, dimension).getFirst();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void testSearch(Blackhole blackhole) throws IOException {
        blackhole.consume(KnnService.knn(target, k, constructSourceFilePath(n, dimension), leafSize));
    }
}
