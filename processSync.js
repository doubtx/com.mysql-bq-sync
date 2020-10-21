const fs = require("fs")
const mysql = require('promise-mysql');
const { SecretManagerServiceClient } = require('@google-cloud/secret-manager');
const { Storage } = require('@google-cloud/storage');
const { BigQuery } = require('@google-cloud/bigquery');
const secrets = new SecretManagerServiceClient();
const storage = new Storage().bucket("mysql-sync-queries");

async function accessSecretVersion(name) {
    const [version] = await secrets.accessSecretVersion({ name: name });
    return version.payload.data.toString();
}

exports.processSyncHandler = async (message, context) => {
    let { credentialsKey, queryPath, targetTable, writeDisposition, targetDataset } = JSON.parse(Buffer.from(message.data, 'base64').toString())
    let credentials = await accessSecretVersion(`projects/265066643162/secrets/${credentialsKey}/versions/latest`)

    let bq = (new BigQuery()).dataset(targetDataset || "Moin_Sync_MySQL")
    console.log(`Syncing query: ${queryPath} to table ${targetTable}: Connecting to source`)
    let mysqlConnection = await mysql.createConnection(JSON.parse(credentials));
    let gcsData = await new Storage().bucket("mysql-sync-queries").file(queryPath).download();

    console.log(`Syncing query: ${queryPath} to table ${targetTable}: Streaming data from source to destination`)
    let gcsFilename = "LOADS/" + targetTable + ".json"
    let gcsStream = await storage.file(gcsFilename).createWriteStream()
    await mysqlConnection.queryStream(gcsData.toString()).on('result', async function(row, index) {
        await gcsStream.write(JSON.stringify(row) + "\n")
    }).on('end', () => {
        mysqlConnection.end()
        gcsStream.end()
    })

    console.log(`Syncing query: ${queryPath} to table ${targetTable}: Running load job`)
    await bq.table(targetTable).load(storage.file(gcsFilename), {
        sourceFormat: 'NEWLINE_DELIMITED_JSON',
        writeDisposition: writeDisposition,
        location: 'us-east4',
        autodetect: true,
    })

    mysqlConnection.destroy()
};