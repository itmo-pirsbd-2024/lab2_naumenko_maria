package ru.nms.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import ru.nms.exceptions.BallTreeFileFormatMismatchException;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static ru.nms.utils.Constants.*;

@Slf4j
public class BallTreeUtils {
    public static RealVector calculateCentroid(FileChannel fileChannel, int dimension, long totalVectors) throws IOException {
        setPositionToStartOfVectorData(fileChannel, dimension);
        RealVector sum = new ArrayRealVector(dimension);

        for (int i = 0; i < totalVectors; i++) {
            RealVector vector = readVectorFromFile(fileChannel, dimension);
            sum = sum.add(vector);
        }
        return sum.mapDivide(totalVectors);
    }

    public static double calculateRadius(FileChannel fileChannel, int dimension, long totalVectors, RealVector centroid) throws IOException {
        setPositionToStartOfVectorData(fileChannel, dimension);

        double radius = 0;
        for (int i = 0; i < totalVectors; i++) {
            RealVector vector = readVectorFromFile(fileChannel, dimension);
            radius = Math.max(radius, vector.getDistance(centroid));
        }
        return radius;
    }

    public static RealVector findFarthestVector(FileChannel fileChannel, RealVector centroid, int dimension, long totalVectors) throws IOException {
        setPositionToStartOfVectorData(fileChannel, dimension);

        RealVector farthestVector = null;
        double maxDistance = 0.0;

        for (int i = 0; i < totalVectors; i++) {
            RealVector vector = readVectorFromFile(fileChannel, dimension);
            double distance = centroid.getDistance(vector);
            if (distance > maxDistance) {
                maxDistance = distance;
                farthestVector = vector;
            }
        }
        return farthestVector;
    }

    public static RealVector findSecondFarthestVector(FileChannel fileChannel, RealVector centroid, RealVector farthestVector, int dimension, long totalVectors) throws IOException {
        setPositionToStartOfVectorData(fileChannel, dimension);

        RealVector secondFarthestVector = null;
        double maxDistance = 0.0;
        for (int i = 0; i < totalVectors; i++) {
            RealVector vector = readVectorFromFile(fileChannel, dimension);

            if (Arrays.equals(vector.toArray(), farthestVector.toArray())) continue;

            double distance = centroid.getDistance(vector) + vector.getDistance(farthestVector);
            if (distance > maxDistance) {
                maxDistance = distance;
                secondFarthestVector = vector;
            }
        }
        return secondFarthestVector;
    }

    public static RealVector findApproximateMedianByBaseLine(FileChannel fileChannel, RealVector baseLine, int dimension, long totalVectors, int sampleSize) throws IOException {
        setPositionToStartOfVectorData(fileChannel, dimension);

        List<RealVector> sampleVectors = extractSample(fileChannel, dimension, totalVectors, sampleSize).stream()
                .map(vector -> vector.projection(baseLine))
                .sorted(PROJECTION_COMPARATOR)
                .toList();

        return sampleVectors.get(sampleVectors.size() / 2);
    }

    public static void verifyFileFormat(FileChannel fileChannel, String fileName) throws IOException {
        fileChannel.position(0);
        ByteBuffer headerBuffer = ByteBuffer.allocate(BALL_TREE_HEADER_SIZE).order(ByteOrder.BIG_ENDIAN);
        fileChannel.read(headerBuffer);
        headerBuffer.flip();
        String headerString = new String(headerBuffer.array(), StandardCharsets.UTF_8);
        if (!BALL_TREE_HEADER.equals(headerString)) {
            throw new BallTreeFileFormatMismatchException(fileName, headerString);
        }
    }

    public static void distributeVectorsBasedOnMedian(FileChannel originalFileChannel, RandomAccessFile leftFile, RandomAccessFile rightFile,
                                                      RealVector baseLine, RealVector median, int dimension, long totalVectors) throws IOException {
        setPositionToStartOfVectorData(originalFileChannel, dimension);
        setPositionToStartOfVectorData(leftFile.getChannel(), dimension);
        setPositionToStartOfVectorData(rightFile.getChannel(), dimension);

        for (int i = 0; i < totalVectors; i++) {
            RealVector vector = readVectorFromFile(originalFileChannel, dimension);
            RealVector projection = vector.projection(baseLine);

            if (PROJECTION_COMPARATOR.compare(projection, median) > 0) {
                putVectorToFile(rightFile, vector);
            } else {
                putVectorToFile(leftFile, vector);
            }
        }
        fillMetaData(leftFile, dimension);
        fillMetaData(rightFile, dimension);
    }

    public static void setPositionToStartOfVectorData(FileChannel fileChannel, int dimension) throws IOException {
        fileChannel.position(
                calculateMetadataSize(dimension)
        );
    }

    private static void fillMetaData(RandomAccessFile file, int dimension) throws IOException {
        long metaDataSize = calculateMetadataSize(dimension);
        long writtenVectorsAmount = (file.getFilePointer() - metaDataSize) / dimension / Double.BYTES;
        file.seek(0);

        file.write(BALL_TREE_HEADER.getBytes(StandardCharsets.UTF_8));
        file.writeLong(writtenVectorsAmount);
        file.writeInt(dimension);
        RealVector centroid = calculateCentroid(file.getChannel(), dimension, writtenVectorsAmount);
        double radius = calculateRadius(file.getChannel(), dimension, writtenVectorsAmount, centroid);

        file.seek(RADIUS_POSITION);
        file.writeDouble(radius);
        putVectorToFile(file, centroid);

        log.info("filling metadata for {} with totalVectors: {}, dimension: {}, radius: {}",
                file.getFD(), writtenVectorsAmount, dimension, radius);
    }

    private static List<RealVector> extractSample(FileChannel fileChannel, int dimension, long totalVectors, int sampleSize) throws IOException {
        setPositionToStartOfVectorData(fileChannel, dimension);
        int vectorsToRead = (int) Math.min(sampleSize / getVectorSize(dimension), totalVectors);
        List<RealVector> sampleVectors = new ArrayList<>(vectorsToRead);

        for (int i = 0; i < vectorsToRead; i++) {
            sampleVectors.add(readVectorFromFile(fileChannel, dimension));
        }
        return sampleVectors;
    }

    public static void putVectorToFile(RandomAccessFile file, RealVector vector) throws IOException {
        double[] values = vector.toArray();
        for (double val : values) file.writeDouble(val);
    }

    public static RealVector readVectorFromFile(FileChannel fileChannel, int dimension) throws IOException {
        double[] arr = readNVectorsFromFile(fileChannel, ByteBuffer.allocate(getVectorSize(dimension)).order(ByteOrder.BIG_ENDIAN));
        return new ArrayRealVector(arr);
    }

    private static long calculateMetadataSize(int dimension) {
        return BALL_TREE_HEADER_SIZE                // file format header
                + META_DATA_HEADER_SIZE             // dimension + total vectors amount
                + RADIUS_SIZE                       // radius
                + getVectorSize(dimension)          // centroid vector
                + Long.BYTES                        // left child file name
                + Long.BYTES;                       // right child file name
    }

    public static double[] readVectorsFromBuffer(int bytesRead, ByteBuffer buffer) {
        double[] doubleBuffer = new double[bytesRead / Double.BYTES];
        buffer.asDoubleBuffer().get(doubleBuffer);
        return doubleBuffer;
    }

    public static double[] readNVectorsFromFile(FileChannel fileChannel, ByteBuffer buffer) throws IOException {
        buffer.clear();
        int bytesRead = fileChannel.read(buffer);

        if (bytesRead == -1) throw new RuntimeException("Unexpected end of file");
        buffer.flip();

        return readVectorsFromBuffer(bytesRead, buffer);
    }

    public static void setChildNodeNames(RandomAccessFile file, int dimension, long leftChildName, long rightChildName) throws IOException {
        file.seek(CENTROID_POSITION + getVectorSize(dimension));
        file.writeLong(leftChildName);
        file.writeLong(rightChildName);
    }


    public static int getVectorSize(int dimension) {
        return dimension * Double.BYTES;
    }
}
