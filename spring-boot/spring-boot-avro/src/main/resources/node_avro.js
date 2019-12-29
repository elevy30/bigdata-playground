'use strict';

function example() {
    const avro = require('avro-js');

    const snappy = require('snappy'); // Or your favorite Snappy library.
    const codecs = {
        snappy: function (buf, cb) {
            // Avro appends checksums to compressed blocks, which we skip here.
            return snappy.uncompress(buf.slice(0, buf.length - 4), cb);
        }
    };

    fs.createReadStream('iris.avro');

    avro.createFileDecoder('iris.avro', {codecs})
        .on('metadata', function (type) { /* `type` is the writer's type. */ })
        .on('data', function (record) { console.log(record)/* Do something with the record. */ });


}

function createFileDecoderFromStream(is, opts) {
    return is.pipe(new avro.BlockDecoder(opts));
}

example();

