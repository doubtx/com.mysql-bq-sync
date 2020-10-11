

const { processSyncHandler } = require("./processSync")

let syncTaskMessageBuff = new Buffer(JSON.stringify({
    credentialsKey: "DEVELOPMENT_MYSQL",
    queryPath: "DEVELOPMENT_MYSQL/developTestTable.sql",
    targetTable: "developTestTable2",
    writeDisposition: "WRITE_TRUNCATE"
}));

console.log(JSON.stringify({
    credentialsKey: "DEVELOPMENT_MYSQL",
    queryPath: "DEVELOPMENT_MYSQL/developTestTable.sql",
    targetTable: "developTestTable2",
    writeDisposition: "WRITE_TRUNCATE"
}))

processSyncHandler({ data: syncTaskMessageBuff.toString('base64') }).then(() => {
    console.log("Test run finished")
})