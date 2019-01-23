const express = require('express');
const bodyParser = require('body-parser');
const morgan    = require('morgan');
const cors=require('cors');
const app=express();

app.listen(/*process.env.PORT ||*/3000, function(){
  console.log("Server starting on port %d in %s mode", this.address().port, app.settings.env);
});
app.use(bodyParser.urlencoded({extended: false}));
app.use(bodyParser.json());
app.use(cors());

app.use(morgan('dev'));

app.use('/tiles', require('./ServerTile'));
