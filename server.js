
const _ = require('lodash');
const express = require('express');
const compression = require('compression');
const WebSocket = require('ws');
// const DataFrame = require("dataframe-js").DataFrame; // なぜかスタックオーバーフローになるので使わない
const moment = require('moment');
const fetch = require('node-fetch');
// const loki = require('lokijs')

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
    // const db = new loki('bitflyer');
    let executions = []
    const historyHours = 6;

    const normalizeDf = () => {
        const minTime = moment().subtract(historyHours, 'hours').unix()
        executions = _.filter(executions, (obj) => {
            return obj.exec_date_unix >= minTime
        })
    }

    const sleep = async (ms) => {
        return new Promise((resolve) => {
            setTimeout(resolve, ms)
        })
    }

    const addExecutions = (data) => {
        Array.prototype.push.apply(executions, _.compact(_.map(data, (row) => {
            let exec_date = row.exec_date
            if (!exec_date) return void 0
            if (exec_date.slice(-1) !== 'Z') {
                exec_date += 'Z'
            }
            return {
                id: row.id,
                price: row.price,
                size: row.size,
                exec_date: exec_date,
                exec_date_unix: moment(exec_date).unix(),
            }
        })))
    }

    const fetchOldData = async (before) => {
        await sleep(1000);
        const url = `https://api.bitflyer.com/v1/executions?count=1000&product_code=FX_BTC_JPY&before=${before}`
        console.log('fetch ' + url)
        const data = await (await fetch(url)).json();
        addExecutions(data)

        const minTime = moment().subtract(historyHours, 'hours').unix()
        const finished = _.some(data, (obj) => {
            return obj.exec_date_unix < minTime
        })
        if (!finished) {
            await fetchOldData(_.min(_.map(data, 'id')))
        }
    }

    sock.addEventListener("open", e => {
        sock.send('{"method": "subscribe","params": {"channel": "lightning_executions_FX_BTC_JPY" }}');
    });

    sock.addEventListener("message", e => {
        const isFirst = executions.length === 0;
        const data = JSON.parse(e.data).params.message;
        addExecutions(data)

        if (isFirst) {
            fetchOldData(_.min(_.map(executions, 'id')))
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
        // const stream = req.params.stream;

        normalizeDf()
        res.json(_.sortBy(executions, 'id'));
    });
}

createServer({
    port: 50001
})
