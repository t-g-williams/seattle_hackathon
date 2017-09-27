npm install

var server_port = process.env.OPENSHIFT_NODEJS_PORT || 8080;

var server_ip_address = process.env.OPENSHIFT_NODEJS_IP || '127.0.0.1';

var express = require('express');
var app = express();

app.use('/', express.static(__dirname + '/'));

app.listen(server_port, server_ip_address, function () {

    console.log( "Listening on " + server_ip_address + ", server_port " + server_port  );

});


// app.listen(3000, function() { console.log('listening')});
