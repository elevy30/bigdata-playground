package poc.mllib.matrix;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.BlockMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.rdd.RDD;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Created by eyallevy on 13/01/17.
 */
public class MatrixManipulation {

    private final JavaSparkContext context;

    public MatrixManipulation(JavaSparkContext context) {
        this.context = context;
    }

    public void run() {
        IndexedRow[] iRowArray = readMatrixFromFile();
        IndexedRowMatrix matrix = indexRowToMatrix(context, iRowArray);
        BlockMatrix blockMatrix = matrix.toBlockMatrix();
        printMatrix(blockMatrix, "Block Matrix");

        IndexedRow sumOfSquaresRow = getSumOfSquares(matrix);
        IndexedRowMatrix sumOfSquaresMatrix = indexRowToMatrix(context, sumOfSquaresRow);
        BlockMatrix sumOfSquaresBlockMatrix = sumOfSquaresMatrix.toBlockMatrix();
        printMatrix(sumOfSquaresBlockMatrix, "Sum Of Squares Matrix");

        IndexedRow onesRow = generateOnesRow(blockMatrix.numRows());
        IndexedRowMatrix onesMatrix = indexRowToMatrix(context, onesRow);
        BlockMatrix onesBlockMatrix = onesMatrix.toBlockMatrix();
        printMatrix(onesBlockMatrix, "Ones Matrix");

//        BlockMatrix m1 = sumOfSquaresBlockMatrix.transpose().multiply(onesBlockMatrix);
//        printMatrix(m1, "m1");
//
//        BlockMatrix transpose = blockMatrix.transpose();
//        printMatrix(transpose, "Transpose");
//        BlockMatrix m2 = blockMatrix.multiply(transpose);
//        printMatrix(m2, "m2");
//
//
//        BlockMatrix m3 = onesBlockMatrix.transpose().multiply(sumOfSquaresBlockMatrix);
//        printMatrix(m3, "m3");
//
//        BlockMatrix result = m1.subtract(m2).add(m3);
//        printMatrix(result, "result");


        double startTime = System.nanoTime();
        BlockMatrix b = blockMatrix.multiply(blockMatrix.transpose());
        BlockMatrix sumb = b.add(b);
        BlockMatrix result = sumOfSquaresBlockMatrix.transpose().multiply(onesBlockMatrix).subtract(sumb).add(onesBlockMatrix.transpose().multiply(sumOfSquaresBlockMatrix));

        double apply = result.toLocalMatrix().apply(0, 1);
        System.out.println(apply);
        apply = result.toLocalMatrix().apply(1, 0);
        System.out.println(apply);

//        RDD<IndexedRow> rdd = result.toIndexedRowMatrix().rows();
//        rdd.saveAsTextFile("out_file.csv");
        double endTime = System.nanoTime();
        System.out.println("Duration = " + (endTime - startTime)/1000000);

        //printMatrix(result, "result");

    }

    private IndexedRow generateOnesRow(long rows) {
        double[] doublesArray = new double[(int) rows];
        for (int i = 0; i < rows; i++) {
            doublesArray[i] = 1d;
        }
        IndexedRow iRow = new IndexedRow(0, Vectors.dense(doublesArray));
        return iRow;
    }

    private IndexedRow[] readMatrixFromFile() {
        List<IndexedRow> iRowList = new ArrayList<>();

        //Get file from resources folder
        ClassLoader classLoader = this.getClass().getClassLoader();
        File file = new File(classLoader.getResource("input_file.csv").getFile());

        try (Scanner scanner = new Scanner(file)) {

            int index = 0;
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                String[] splitL = line.split(",");
                double[] doublesArray = new double[splitL.length];
                for (int i = 0; i < splitL.length; i++) {
                    doublesArray[i] = Double.parseDouble(splitL[i]);
                }
                IndexedRow iRow = new IndexedRow(index, Vectors.dense(doublesArray));
                iRowList.add(iRow);
                index++;
            }

            scanner.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        IndexedRow[] iRowArray = new IndexedRow[iRowList.size()];
        return iRowList.toArray(iRowArray);
    }

    private IndexedRow getSumOfSquares(IndexedRowMatrix matrix) {
        JavaRDD<IndexedRow> indexedRowJavaRDD = matrix.rows().toJavaRDD();
        JavaRDD<Double> doubleJavaRDD = indexedRowJavaRDD.map((IndexedRow row) -> {
            double sum = 0;
            double[] doubles = row.vector().toArray();
            for (double d : doubles) {
                sum = sum + (d * d);
            }
            return sum;
        });

        List<Double> doubles = doubleJavaRDD.collect();
        double[] doublesArray = new double[doubles.size()];
        for (int i = 0; i < doubles.size(); i++) {
            doublesArray[i] = doubles.get(i);
        }
        IndexedRow iRow = new IndexedRow(0, Vectors.dense(doublesArray));
        return iRow;
    }

    private IndexedRowMatrix indexRowToMatrix(JavaSparkContext context, IndexedRow... indexedRows) {
        List<IndexedRow> indexRowList = new ArrayList();
        for (IndexedRow iRow : indexedRows) {
            indexRowList.add(iRow);
        }

        JavaRDD<IndexedRow> indexedRowsRdd = context.parallelize(indexRowList);
        return new IndexedRowMatrix(indexedRowsRdd.rdd());
    }

    private void printMatrix(BlockMatrix blockMatrix, String title) {
        printMatrix(blockMatrix.toIndexedRowMatrix(), title);
    }

    private void printMatrix(IndexedRowMatrix indexedRowMatrix, String title) {
        RDD<Vector> rows = indexedRowMatrix.toRowMatrix().rows();
        List<Vector> vs = rows.toJavaRDD().collect();
        System.out.println("*********** " + title + " ***************");
        for (Vector v : vs) {
            System.out.println(v);
        }
        System.out.println("***************************************");
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SVD Example");
        conf.setMaster("local[8]");
        SparkContext sc = new SparkContext(conf);
        JavaSparkContext context = JavaSparkContext.fromSparkContext(sc);

        MatrixManipulation matrixManipulation = new MatrixManipulation(context);
        matrixManipulation.run();

    }
}
