
function avro() {
    let url = "http://localhost:8080/avro";

    $.ajax({
        url: url,
        type: 'get'
    }).done(function (data, statusText, xhr) {
        let status = xhr.status;                //200
        let head = xhr.getAllResponseHeaders(); //Detail header info
        console.log("data: " + data);
        console.log("head: " + head);


        document.getElementById("results").innerHTML = statusText + ":" + status + "<BR><textarea rows='100' cols='100'>" + data + "</textarea>";
    });

    //
    // $.get(url,
    //     function (data) {
    //         console.log(data);
    //         document.getElementById("div").innerHTML = xmlhttp.statusText + ":" + xmlhttp.status + "<BR><textarea rows='100' cols='100'>" + xmlhttp.responseText + "</textarea>";
    //     });
}

function parquet() {
    let url = "http://localhost:8080/parquet";

    $.get({url,
        dataType: 'binary',
        }).done(function(data, statusText, xhr){
            let status = xhr.status;                //200
            let head = xhr.getAllResponseHeaders(); //Detail header info
            console.log("data: " + data);
            console.log("head: " + head);

            let link=document.createElement('a');
            link.href=URL.createObjectURL(data);
            link.download="/tmp/parquet";
            link.click();

            //
            // let url = URL.createObjectURL(data);
            // let $a = $('<a />', {
            //     'href': url,
            //     'download': '/tmp/parquet',
            //     'text': "click"
            // }).hide().appendTo("body")[0].click();

            // URL.revokeObjectURL(url);




            document.getElementById("results").innerHTML = statusText + ":" + status + "<BR><textarea rows='100' cols='100'>" + data+ "</textarea>";
        });
}

function download(text, name, type) {
    var a = document.getElementById("a");
    var file = new Blob([text], {type: type});
    a.href = URL.createObjectURL(file);
    a.download = name;
}

function callbackFunction(msg) {
    alert(msg);
}