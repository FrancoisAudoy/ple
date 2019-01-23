const express = require('express');
const router = express.Router();
const Database = require('./Databse');

router.get("/:x/:y/:z", async function(){
  let test=await Database.requestSchema();
  res.status(200).json(test);
});
