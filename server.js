
const _ = require('lodash');
const express = require('express');
const compression = require('compression');
const WebSocket = require('ws')
const ReconnectingWebSocket = require('reconnecting-websocket');
// const DataFrame = require("dataframe-js").DataFrame; // なぜかスタックオーバーフローになるので使わない
const moment = require('moment');
const fetch = require('node-fetch');
// const loki = require('lokijs')

const sleep = async (ms) => {
    return new Promise((resolve) => {
        setTimeout(resolve, ms)
    })
}

function createServer(config) {
    const app = initServer(config);
    initWSClient(app, config);
    return app;
};

// https://stackoverflow.com/questions/37318808/what-is-the-in-place-alternative-to-array-prototype-filter
function filterInPlace(a, condition) {
    let i = 0, j = 0;

    while (i < a.length) {
        const val = a[i];
        if (condition(val, i, a)) a[j++] = val;
        i++;
    }

    a.length = j;
    return a;
}

const uniqueByKeyInPlaceSorted = (a, key) => {
    let i = 0, j = 0;
    const last = a.length - 1

    while (i < a.length) {
        const val = a[i];
        if (i === last || val[key] !== a[i + 1][key]) a[j++] = val;
        i++;
    }

    a.length = j;
    return a;
}

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
    const sock = new ReconnectingWebSocket(
        'wss://ws.lightstream.bitflyer.com/json-rpc',
        [],
        {
            WebSocket: WebSocket,
            connectionTimeout: 1000,
            maxRetries: 10,
        }
    )
    // const db = new loki('bitflyer');
    const symbolExecutions = {
        'FX_BTC_JPY': [],
        'BTC_JPY': [],
    }
    let prevLengthAfterRemove = 0
    const historyHours = 24

    const idIndex = 0
    const priceIndex = 1
    const sizeIndex = 2
    const execDateIndex = 3
    const execDateUnixIndex = 4

    const removeOld = () => {
        prevLengthAfterRemove = 0
        _.each(symbolExecutions, (executions, symbol) => {
            const minTime = moment().subtract(historyHours, 'hours').unix()
            // メモリ節約のためにinplace
            filterInPlace(executions, (obj) => {
                return obj[execDateUnixIndex] >= minTime
            })
            prevLengthAfterRemove += executions.length

            // 重複排除
            executions.sort()
            uniqueByKeyInPlaceSorted(executions, idIndex)
        })

        const memUsage = process.memoryUsage()
        const memStr = []
        for (const key in memUsage) {
            memStr.push(`${key} ${Math.round(memUsage[key] / 1024 / 1024)} MB`)
        }
        console.log(`removeOld -> ${prevLengthAfterRemove}. memory usage ${memStr.join(' ')}`)
    }

    const normalizeExecDate = (exec_date) => {
        if (!exec_date) return exec_date
        if (exec_date.slice(-1) !== 'Z') {
            exec_date += 'Z'
        }
        return exec_date
    }

    const addExecutions = (symbol, data) => {
        Array.prototype.push.apply(symbolExecutions[symbol], _.compact(_.map(data, (row) => {
            const exec_date = normalizeExecDate(row.exec_date)
            if (!exec_date) return void 0
            return [
                row.id,
                row.price,
                row.size,
                exec_date,
                moment(exec_date).unix(),
            ]
        })))

        const len = _.sum(_.map(symbolExecutions, 'length'))
        if (len > 1.1 * prevLengthAfterRemove) {
            removeOld()
        }
    }

    const logStatus = () => {
        _.each(symbolExecutions, (executions, symbol) => {
            const minDate = _.min(_.map(executions, execDateUnixIndex))

            console.log(`${symbol} count ${executions.length} oldest ${moment.unix(minDate).utc().format()}`)
        })
    }
    setInterval(logStatus, 10 * 1000)

    const healthcheck = () => {
        _.each(symbolExecutions, (executions, symbol) => {
            const maxDate = _.max(_.map(executions, execDateUnixIndex))

            if (maxDate < moment().unix() - 15 * 60) {
                console.log('data not updated. restarting')
                process.exit()
            }
        })
    }
    setInterval(healthcheck, 60 * 1000)

    const fetchOldData = async (symbol, before) => {
        await sleep(2000);
        let url = `https://api.bitflyer.com/v1/executions?count=1000&product_code=${symbol}`
        if (before) {
            url += `&before=${before}`
        }
        console.log('fetch ' + url)
        const data = await (await fetch(url)).json();
        addExecutions(symbol, data)

        const minTime = moment().subtract(historyHours, 'hours').unix()
        const finished = _.some(data, (obj) => {
            return moment(normalizeExecDate(obj.exec_date)).unix() < minTime
        })
        if (!finished) {
            await fetchOldData(symbol, _.min(_.map(data, 'id')))
        }
    }

    sock.addEventListener("open", e => {
        console.log('websocket open')
        sock.send('{"method": "subscribe","params": {"channel": "lightning_executions_FX_BTC_JPY" }}');
        sock.send('{"method": "subscribe","params": {"channel": "lightning_executions_BTC_JPY" }}');
    });

    sock.addEventListener("message", e => {
        const params = JSON.parse(e.data).params
        const channel = params.channel
        const data = params.message

        const symbol = channel.replace('lightning_executions_', '')
        const isFirst = symbolExecutions[symbol].length === 0;

        addExecutions(symbol, data)

        console.log(`websocket data arrived ${symbol} ${data.length}`)

        if (isFirst) {
            fetchOldData(symbol, _.min(_.map(symbolExecutions[symbol], 'id')))
        }
    });

    sock.addEventListener('error', function(error) {
        console.error("Caught Websocket error:", error);
    });

    sock.addEventListener('end', function() {
        console.error('Client closed due to unrecoverable WebSocket error. Please check errors above.');
    });

    app.get('/executions', function(req, res) {
        const symbol = req.query.symbol || 'FX_BTC_JPY';
        const executions = symbolExecutions[symbol]
        console.log(`GET executions ${symbol}`)

        // メモリ節約のためにinplace
        executions.sort()

        res.write('{\n')

        res.write('"id": ')
        res.write(JSON.stringify(_.map(executions, idIndex)))
        res.write(',\n')

        res.write('"price": ')
        res.write(JSON.stringify(_.map(executions, priceIndex)))
        res.write(',\n')

        res.write('"size": ')
        res.write(JSON.stringify(_.map(executions, sizeIndex)))
        res.write(',\n')

        res.write('"exec_date": ')
        res.write(JSON.stringify(_.map(executions, execDateIndex)))

        res.write('\n}\n')
        res.end()
    });
}

createServer({
    port: 50001
})
