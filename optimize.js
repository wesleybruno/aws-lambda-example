'use strict'

const { MongoClient } = require('mongodb')

async function connectSourceDatabase () {
  try {
    console.log('=> connect to source database')
    console.log('=> MONGODB_URI: ' + process.env.MONGO_SOURCE_URL)

    const mongoClient = await MongoClient.connect(process.env.MONGO_SOURCE_URL, {
      useNewUrlParser: true,
      useUnifiedTopology: true
    })
    console.log('=> MONGODB_SOURCE_DATABASE: ' + process.env.MONGODB_SOURCE_DATABASE)
    return mongoClient.db(process.env.MONGODB_SOURCE_DATABASE)
  } catch (ex) {
    return ex
  }
}

async function connectDestinyDatabase () {
  try {
    console.log('=> connect to destiny database')
    console.log('=> MONGODB_URI: ' + process.env.MONGO_URL)

    const mongoClient = await MongoClient.connect(process.env.MONGO_URL, {
      useNewUrlParser: true,
      useUnifiedTopology: true
    })
    console.log('=> MONGODB_DATABASE: ' + process.env.MONGODB_DATABASE)
    return mongoClient.db(process.env.MONGODB_DATABASE)
  } catch (ex) {
    return ex
  }
}

async function querySourceDatabase (db) {
  try {
    console.log('=> query source database')
    return db.collection('usuario').countDocuments({
      municipioId: { $exists: true }
    })
  } catch (ex) {
    return ex
  }
}

async function queryDestinyDatabase (db, value) {
  try {
    console.log('=> query destiny database')
    const dataUpdate = {}
    const insertResult = await db.collection('relatorios').insertOne(dataUpdate)
    if (insertResult.result.ok) {
      console.log('Dados atualizados')
      return dataUpdate
    }
    return false
  } catch (ex) {
    return ex
  }
}

module.exports.handle = async (event) => {
  try {
    const dbSource = await connectSourceDatabase()
    const queryValue = await querySourceDatabase(dbSource)

    const dbDestiny = await connectDestinyDatabase()
    const dataUpdate = await queryDestinyDatabase(dbDestiny, queryValue)
    return {
      statusCode: 200,
      body: JSON.stringify(
        {
          message: 'Connection succefull',
          data: dataUpdate
        }
      )
    }
  } catch (ex) {
    return ex
  }
}
