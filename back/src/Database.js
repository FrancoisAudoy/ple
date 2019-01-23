module.exports= class Database{

  async requestSchema(table){
      return require('http://young:8000/'+table+'/schema');
  }

}
