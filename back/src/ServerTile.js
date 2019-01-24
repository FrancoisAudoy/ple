const express = require('express');
const router = express.Router();
const hbase=require('hbase');
const pngjs=require('pngjs');
const PNG = require('pngjs').PNG;

let client=hbase({host:'0.0.0.0', port:8080});

router.get("/:x/:y/:z", async function(req, res){
  let row=new hbase.Row(client, 'TilesAF', 'X17811118Y23232576');
  val =row.get("Tile:img", (error, value) => {
    if(!error){
      let png=new PNG({ filterType: 4 }).parse( value[0].$, function(error, data)
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
