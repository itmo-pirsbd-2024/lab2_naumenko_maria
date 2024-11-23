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
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static ru.nms.utils.Constants.*;

@Slf4j
public class BallTreeUtils {

    /**
     * Осознаю, что некрасиво в утильном классе держать объекты,
     * но не хотелось ради одного буффера переделывать предыдущую
     * реализацию
     **/
    private static ByteBuffer vectorsBuffer;

    /**
     * Поиск центроида "шара" с векторами.
     * @param fileChannel файл с векторами.
     * @param dimension размерность векторов.
     * @param totalVectors количество векторов в файле.
     * @return вектор, находящийся в центре шара.
     * @throws IOException
     */
    public static RealVector calculateCentroid(FileChannel fileChannel, int dimension, long totalVectors) throws IOException {
        setPositionToStartOfVectorData(fileChannel, dimension);
        RealVector sum = new ArrayRealVector(dimension);

        for (int i = 0; i < totalVectors; i++) {
            RealVector vector = readVectorFromFile(fileChannel, dimension);
            sum = sum.add(vector);
        }
        return sum.mapDivide(totalVectors);
    }

    /**
     * Поиск радиуса шара.
     * @param fileChannel файл с векторами.
     * @param dimension размерность векторов.
     * @param totalVectors количество векторов в файле.
     * @param centroid центроид шара.
     * @return радиус.
     * @throws IOException
     */
    public static double calculateRadius(FileChannel fileChannel, int dimension, long totalVectors, RealVector centroid) throws IOException {
        setPositionToStartOfVectorData(fileChannel, dimension);

        double radius = 0;
        for (int i = 0; i < totalVectors; i++) {
            RealVector vector = readVectorFromFile(fileChannel, dimension);
            radius = Math.max(radius, vector.getDistance(centroid));
        }
        return radius;
    }

    /**
     * Поиск вектора, наиболее удаленного от центра шара.
     * @param fileChannel файл с векторами.
     * @param centroid центроид.
     * @param dimension размерность векторов.
     * @param totalVectors количество векторов в файле.
     * @return набиолее удаленный вектор.
     * @throws IOException
     */
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

    /**
     * Поиск второй точки искомого отрезка, где первая точка
     * это самый удаленный от центра вектор. Отрезок в иделе должен
     * быть диаметром, поэтому необходимо найти вектор, наиболее удаленный
     * от центроида и от первой точки.
     * @param fileChannel файл с векторами.
     * @param centroid центроид.
     * @param farthestVector набиолее удаленный вектор от центроида.
     * @param dimension размерность векторов.
     * @param totalVectors количество векторов в файле.
     * @return вектор, наиболее удаленный от центроида и первой точки отрезка.
     * @throws IOException
     */
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

    /**
     * Распределяет вектора на две приблизительно равные части и не пересекающиеся в
     * пространстве части засчет проецирования векторов на отрезок и сравнение
     * проекций с серединой этого отрезка.
     * @param originalFileChannel файл с векторами.
     * @param leftFile файл левого подмножества векторов.
     * @param rightFile файл правого подмножества векторов.
     * @param baseLine отрезок между двумя наиболее удаленными от центра и от друг друга векторами.
     * @param median середина отрезка.
     * @param dimension размерность векторов.
     * @param totalVectors количество векторов в файле.
     * @throws IOException
     */
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

    public static void setChildNodeNames(RandomAccessFile file, int dimension, long leftChildName, long rightChildName) throws IOException {
        file.seek(CENTROID_POSITION + getVectorSize(dimension));
        file.writeLong(leftChildName);
        file.writeLong(rightChildName);
    }

    public static int getVectorSize(int dimension) {
        return dimension * Double.BYTES;
    }

    public static void setPositionToStartOfVectorData(FileChannel fileChannel, int dimension) throws IOException {
        fileChannel.position(
                calculateMetadataSize(dimension)
        );
    }

    public static void putVectorToFile(RandomAccessFile file, RealVector vector) throws IOException {
        double[] values = vector.toArray();
        for (double val : values) file.writeDouble(val);
    }

    public static RealVector readVectorFromFile(FileChannel fileChannel, int dimension) throws IOException {
        double[] arr = readNVectorsFromFile(fileChannel, ByteBuffer.allocate(getVectorSize(dimension)).order(ByteOrder.BIG_ENDIAN));
        return new ArrayRealVector(arr);
    }

    /**
     * Вообще говоря надо было и построение дерева переделать на чтение/запись батчами,
     * но так как скорость построение дерева нас не особо интересует в данном случае,
     * то на стала тратить на это время, так как суть оптимизации и так ясна на
     * экспериментах с поиском по дереву.
     **/
    public static List<RealVector> readVectorsFromFile(FileChannel fileChannel, int dimension, int vecAmount) throws IOException {
        double[] vectorsArray = readNVectorsFromFile(fileChannel, getBuffer(dimension, vecAmount));
        int finalVecAmount = (vectorsArray.length + dimension - 1) / dimension;
        return IntStream.range(0, finalVecAmount)
                .parallel()
                .mapToObj(i -> Arrays.copyOfRange(vectorsArray, i * dimension, Math.min(vectorsArray.length, (i + 1) * dimension)))
                .map(array -> (RealVector) new ArrayRealVector(array))
                .toList();
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

        //log.info("filling metadata for {} with totalVectors: {}, dimension: {}, radius: {}",
//                file.getFD(), writtenVectorsAmount, dimension, radius);
    }


    private static long calculateMetadataSize(int dimension) {
        return BALL_TREE_HEADER_SIZE                // file format header
                + META_DATA_HEADER_SIZE             // dimension + total vectors amount
                + RADIUS_SIZE                       // radius
                + getVectorSize(dimension)          // centroid vector
                + Long.BYTES                        // left child file name
                + Long.BYTES;                       // right child file name
    }

    private static double[] readVectorsFromBuffer(int bytesRead, ByteBuffer buffer) {
        double[] doubleBuffer = new double[bytesRead / Double.BYTES];
        buffer.asDoubleBuffer().get(doubleBuffer);
        return doubleBuffer;
    }

    private static double[] readNVectorsFromFile(FileChannel fileChannel, ByteBuffer buffer) throws IOException {
        buffer.clear();
        int bytesRead = fileChannel.read(buffer);

        if (bytesRead == -1) throw new RuntimeException("Unexpected end of file");
        buffer.flip();

        return readVectorsFromBuffer(bytesRead, buffer);
    }

    private static ByteBuffer getBuffer(int dimension, int nodeSize) {
        if (vectorsBuffer == null)
            vectorsBuffer = ByteBuffer.allocate(getVectorSize(dimension) * nodeSize).order(ByteOrder.BIG_ENDIAN);
        return vectorsBuffer;
    }
}
