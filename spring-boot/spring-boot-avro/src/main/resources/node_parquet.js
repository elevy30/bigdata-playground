'use strict';

//let parquetjs = require('parquetjs');
let fs = require('fs');

function example() {


// create new ParquetReader that reads from 'fruits.parquet`
    //let reader = await (parquetjs.ParquetReader.openFile('iris.parquet'));

    let fileDescriptor = fopen('iris.parquet');

// create a new cursor
    let cursor = reader.getCursor();

// read all records from the file and print them
    let record = null;
    while (record = await( cursor.next())) {
        console.log(record);
    }
}

exports.fopen = function(filePath) {
    return new Promise((resolve, reject) => {
        fs.open(filePath, 'r', (err, fd) => {
            if (err) {
                reject(err);
            } else {
                resolve(fd);
            }
        })
    });
}

example();

