'use strict';

const parquetjs = require('parquetjs');

async function example() {
    try {
        let reader = await parquetjs.ParquetReader.openFile('iris.parquet');

        // create a new cursor
        let cursor = reader.getCursor();
        console.log(cursor);

        // read all records from the file and print them
        let record;
        while (record = await( cursor.next())) {
            console.log(record);
        }
    } catch (e){
        console.log(e); // or breakpoint, etc
        throw e;
    }


}

example();

