const express = require('express');
const router = express.Router();
const hbase=require('hbase');
const pngjs=require('pngjs');
const PNG = require('pngjs').PNG;

let client=hbase({host:'0.0.0.0', port:3001});

router.get("/:x/:y/:z", async function(req, res){
  let row=new hbase.Row(client, 'TilesAF', 'X0Y1006021Z1');
  val =row.get("Tile:img", (error, value) => {
    if(!error){
      console.log(value);
      let png=new PNG().parse( value[0].$, function(error, data)
      {
        console.log(error, data)
      });
      res.status(200).send(png);
    }else{
      res.status(404).end();
    }
  });
});

module.exports = router;
