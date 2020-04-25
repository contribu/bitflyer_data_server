
const _ = require('lodash');
const express = require('express');
const compression = require('compression');
const WebSocket = require('ws');
const DataFrame = require("dataframe-js").DataFrame;
const moment = require('moment');
const fetch = require('node-fetch');

function createServer(config) {
    const app = initServer(config);
    initWSClient(app, config);
    return app;
};

function initServer(config) {
    const app = express();

    app.use(compression());

    // Readability
    if (process.env.NODE_ENV !== 'production') {
        app.set('json spaces', 2);
    }

    app.listen(config.port, function() {
        console.log('Bitflyer data server listening on http://localhost:' + config.port);
    });

    return app;
}

function initWSClient(app, config) {
    const sock = new WebSocket('wss://ws.lightstream.bitflyer.com/json-rpc');
    let df_executions = void 0;
    const historyHours = 12;

    const normalizeDf = () => {
        const minTime = moment().subtract(historyHours, 'hours').unix()
        df_executions = df_executions
            .filter(row => row.get('exec_date_unix') >= minTime)
            .dropDuplicates('id')
            .sortBy('id');
    }

    const sleep = async (ms) => {
        return new Promise((resolve) => {
            setTimeout(resolve, ms)
        })
    }

    const fetchOldData = async (before) => {
        await sleep(1000);
        const url = `https://api.bitflyer.com/v1/executions?count=1000&product_code=FX_BTC_JPY&before=${before}`
        console.log('fetch ' + url)
        const data = await (await fetch(url)).json();
        let df = new DataFrame(data, ['id', 'price', 'size', 'exec_date']);
        df = df.withColumn('exec_date_unix', (row) => moment(row.get('exec_date')).unix())
        df_executions = df_executions.union(df);

        const minTime = moment().subtract(historyHours, 'hours').unix()
        const oldDf = df.filter(row => row.get('exec_date_unix') < minTime)
        if (oldDf.count() == 0) {
            await fetchOldData(df.stat.min('id'))
        }
    }

    sock.addEventListener("open", e => {
        sock.send('{"method": "subscribe","params": {"channel": "lightning_executions_FX_BTC_JPY" }}');
    });

    sock.addEventListener("message", e => {
        const data = JSON.parse(e.data).params.message;
        let df = new DataFrame(data, ['id', 'price', 'size', 'exec_date']);
        df = df.withColumn('exec_date_unix', (row) => moment(row.get('exec_date')).unix())

        if (df_executions) {
            df_executions = df_executions.union(df);
        } else {
            df_executions = df;
            fetchOldData(df_executions.stat.min('id'))
        }
    });

    sock.addEventListener('error', function(error) {
        console.error("Caught Websocket error:", error);
    });

    sock.addEventListener('end', function() {
        console.error('Client closed due to unrecoverable WebSocket error. Please check errors above.');
        process.exit(1);
    });

    app.get('/executions', function(req, res) {
        const stream = req.params.stream;

        normalizeDf()
        res.json(df_executions.toDict());
    });
}

createServer({
    port: 50001
})
