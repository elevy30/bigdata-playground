package com.tr.jobs.datasource.upload.services;

import au.com.bytecode.opencsv.CSVWriter;
import com.tr.commons.constant.Constants;
import com.tr.commons.model.DataFieldItem;
import com.tr.commons.service.HDFSPersistenceServiceUpload;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static scala.collection.JavaConversions.seqAsJavaList;

/**
 * Created by eyallevy on 07/05/17
 */
@Slf4j
@AllArgsConstructor
@SuppressWarnings("Duplicates")
class SparkSequenceService {

    private SparkSession sparkSession;
    private HDFSPersistenceServiceUpload dataPersistenceService;

    private int featureIndexThreshold;

    Dataset<Row> addSequenceColumnToDS(JavaRDD<String[]> nonCorruptedLines, Broadcast<List<DataFieldItem>> bHeaders, String tempPath, Broadcast<String> bDelimiter, Broadcast<Long> bOffset) {
        log.debug("Adding sequence column To DataSource");
        Dataset<Row> dataSourceWithSeq;



        int numOfCol = bHeaders.getValue().size();
        log.info("DataSource: index threshold was set to {} while #OfFeature is {}", featureIndexThreshold, numOfCol);

        if(numOfCol > featureIndexThreshold) {
            //Add sequence ID column while writing and reading the csv
            log.info("DataSource: Generate SEQ using WR CSV");
            dataSourceWithSeq = addDSSequenceUsingWriteRead(sparkSession, nonCorruptedLines, Constants.COLUMN_NAME_TR_SEQ, bHeaders, tempPath, bDelimiter, bOffset);
        }else{
            //Add sequence ID column while using createDataFrame()
            log.info("DataSource: Generate SEQ using spark.createDataFrame()");
            JavaRDD<Row> nonCorruptedLinesRow = nonCorruptedLines.map(RowFactory::create);

            List<StructField> fields = bHeaders.getValue().stream()
                    .sorted(Comparator.comparingInt(DataFieldItem::getPosition))
                    .map(df -> DataTypes.createStructField(df.getName(), DataTypes.StringType, true))
                    .collect(Collectors.toList());

            dataSourceWithSeq = addSequenceColumn(sparkSession, nonCorruptedLinesRow, fields, Constants.COLUMN_NAME_TR_SEQ, bOffset);
        }
        return dataSourceWithSeq;
    }

    Dataset<Row> addSequenceColumnToDataSet(Dataset<Row> rowDataset, String tempPath, Broadcast<Long> bOffset) {
        log.debug("Adding sequence column To DataFrame");
        Dataset<Row> dataSetWithSeq;

        int numOfCol = rowDataset.schema().fields().length;
        log.info("DataFrame: index threshold was set to {} while #OfFeature is {}", featureIndexThreshold, numOfCol);

        if (numOfCol > featureIndexThreshold) {
            //Add sequence ID column while writing and reading the csv
            log.info("DataFrame: Generate SEQ using WR CSV");
            dataSetWithSeq = addSequenceUsingWriteRead(sparkSession, rowDataset, Constants.COLUMN_NAME_TR_SEQ, tempPath, bOffset);
        } else {
            //Add sequence ID column while using createDataFrame()
            log.info("DataFrame: Generate SEQ using spark.createDataFrame()");
            JavaRDD<Row> rddOfRow = rowDataset.toJavaRDD();
            List<StructField> fields = new ArrayList<>(Arrays.asList(rowDataset.schema().fields()));
            dataSetWithSeq = addSequenceColumn(sparkSession, rddOfRow, fields, Constants.COLUMN_NAME_TR_SEQ, bOffset);
        }
        return dataSetWithSeq;
    }

    private Dataset<Row> addSequenceUsingWriteRead(SparkSession sparkSession, Dataset<Row> dataFrameToSave, String idColumnName, String tempPath, Broadcast<Long> bOffset) {
        //Add index to RDD
        log.debug("DataFrame: Add index to RDD using addDFSequenceUsingWriteRead");
        JavaPairRDD<Row, Long> rowLongJavaPairRDD = dataFrameToSave.toJavaRDD().zipWithIndex();
        JavaRDD<String> rddWithIndex = rowLongJavaPairRDD.map(t -> {
            Row row = t._1();
            List<Object> values = seqAsJavaList(row.toSeq());
            String[] strValues = values.stream().map(Object::toString).toArray(String[]::new);
            StringWriter sWriter = new StringWriter();
            CSVWriter csvWriter = new CSVWriter(sWriter);
            csvWriter.writeNext(strValues);
            csvWriter.close();
            return (t._2() + bOffset.getValue()) + "," + sWriter.toString();
        });
        writeRDDAsCsv(tempPath, rddWithIndex);

        //add the sequence id to the list of field as the first field
        StructField[] fields = dataFrameToSave.schema().fields();
        StructType dataSourceSchema = buildStructType(idColumnName, fields);
        Dataset<Row> csvDataSet = readCsvAsDataset(sparkSession, tempPath, dataSourceSchema);

        return replaceNullValues(csvDataSet);
    }

    private Dataset<Row> addDSSequenceUsingWriteRead(SparkSession sparkSession, JavaRDD<String[]> nonCorruptedLines, String idColumnName, Broadcast<List<DataFieldItem>> bHeaders, String tempPath, Broadcast<String> bDelimiter, Broadcast<Long> bOffset) {
        //Add index to RDD
        log.debug("DataSource: Add index to RDD using addDSSequenceUsingWriteRead");
        JavaPairRDD<String[], Long> rddPair = nonCorruptedLines.zipWithIndex();
        JavaRDD<String> rddWithIndex = rddPair.map(t -> {
            String[] splits = t._1();
            StringBuilder sb = new StringBuilder(String.valueOf(t._2() + bOffset.getValue()));
            Arrays.stream(splits).forEach(token -> {
                sb.append(bDelimiter.getValue());
                sb.append(token);
            });
            return sb.toString();
        });
        writeRDDAsCsv(tempPath, rddWithIndex);

        List<DataFieldItem> headers = bHeaders.getValue();
        StructField[] fields = headers.stream()
                .sorted(Comparator.comparingInt(DataFieldItem::getPosition))
                .map(df -> DataTypes.createStructField(df.getName(), DataTypes.StringType, true))
                .toArray(StructField[]::new);
        StructType dataSourceSchema = buildStructType(idColumnName, fields);

        //read csv as dataset
        Dataset<Row> csvDataSet = readCsvAsDataset(sparkSession, tempPath, dataSourceSchema);
        Dataset<Row> filteredHeader = csvDataSet.filter(col(fields[0].name()).notEqual(bHeaders.getValue().get(0).getName()));

        return replaceNullValues(filteredHeader);
    }

    private Dataset<Row> addSequenceColumn(SparkSession spark, JavaRDD<Row> rddOfRow, List<StructField> fields, String idColumnName, Broadcast<Long> bOffset) {
        //Add index to RDD
        log.debug("Add index to RDD using addSequenceColumn");
        JavaPairRDD<Row, Long> rddPair = rddOfRow.zipWithIndex();
        JavaRDD<Row> rddWithIndex = rddPair.map(t -> {
            List<Object> origRow = seqAsJavaList(t._1().toSeq());
            // +1 is for the new column of tr sequence
            List<Object> newList = new ArrayList<>(origRow.size() + 1);
            newList.add(t._2() + bOffset.getValue()); //ID
            newList.addAll(origRow); //Rest of row
            return RowFactory.create(newList.toArray());
        });

        //Build new schema for the new dataset
        fields.add(0, new StructField(idColumnName, DataTypes.LongType, true, Metadata.empty()));
        StructField[] extendedFields = fields.toArray(new StructField[0]);
        StructType extendedSchema = new StructType(extendedFields);

        return spark.createDataFrame(rddWithIndex, extendedSchema);
    }

    private void writeRDDAsCsv(String tempPath, JavaRDD<String> rddWithIndex) {
        log.debug("Delete previous temp csv file from {}", tempPath);
        deleteTempPath(tempPath);
        log.debug("Write RDD as a csv to temp folder {}", tempPath);
        rddWithIndex.saveAsTextFile(tempPath);
    }

    private StructType buildStructType(String idColumnName, StructField[] fields) {
        log.debug("Add index column to Schema (StructType)");
        //add the sequence id to the list of field as the first field
        StructField[] fieldsWithId = new StructField[fields.length + 1];
        fieldsWithId[0] = new StructField(idColumnName, DataTypes.LongType, true, Metadata.empty());
        System.arraycopy(fields, 0, fieldsWithId, 1, fields.length);

        return DataTypes.createStructType(fieldsWithId);
    }

    private Dataset<Row> readCsvAsDataset(SparkSession sparkSession, String tempPath, StructType dataSourceSchema) {
        //read csv as dataset
        log.debug("Read csv as DataSet from temp folder {}", tempPath);
        return sparkSession.read()
                .schema(dataSourceSchema)
                .option("header", false)
                .csv(tempPath);
    }

    private Dataset<Row> replaceNullValues(Dataset<Row> csvDataSet) {
        DataFrameNaFunctions functions = new DataFrameNaFunctions(csvDataSet);
        return functions.fill("");
    }

    void deleteTempPath(String tempTextDFWithIndexPath) {
        try {
            dataPersistenceService.deleteTempPath(tempTextDFWithIndexPath);
        } catch (IOException | URISyntaxException e) {
            log.warn("Fail to delete temp csv file from {} : {}",tempTextDFWithIndexPath, e.getMessage());
            e.printStackTrace();
        }
    }
}
