package ru.nms.benchmark;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.linear.RealVector;
import org.openjdk.jmh.annotations.*;
import ru.nms.BallTreeBuilder;

import java.io.IOException;

import static ru.nms.TestDataGenerator.generateNVectors;
import static ru.nms.TestUtils.*;

@Slf4j
@State(Scope.Benchmark)
@Getter
public class ExecutionPlan {

    @Getter
    private RealVector target;

    @Param({"100", "1000", "10000", "100000"})
    private int leafSize;

    @Param({"100"})
    private int k;

    @Param({"1000", "10000", "100000", "1000000"})
    private long n;

    @Param({"256"})
    private int dimension;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        target = generateNVectors(RANDOM, 1, dimension).getFirst();
        BallTreeBuilder.build(constructSourceFilePath(n, dimension), true, leafSize);
    }

    @TearDown(Level.Trial)
    public void clean() {
        cleanTempDir();
    }
}
