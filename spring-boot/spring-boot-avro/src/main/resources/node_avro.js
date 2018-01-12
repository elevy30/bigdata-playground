'use strict';

function example() {
    const avro = require('avro-js');

    avro.createFileDecoder('iris.avro')
        .on('metadata', function (type) { /* `type` is the writer's type. */ })
        .on('data', function (record) { /* Do something with the record. */ });


}

example();

