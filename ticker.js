
const _ = require('lodash');
const express = require('express');
const compression = require('compression');
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
    let ticker = void 0

    const updateTicker = async () => {
        const url = 'https://api.bitflyer.com/v1/getticker?product_code=FX_BTC_JPY'
        console.log('fetch ' + url)
        ticker = await (await fetch(url)).json();
    }

    const callPerSec = 450 / (5 * 60)
    setInterval(updateTicker, 1000 / callPerSec)

    app.get('/ticker', (req, res) => {
        res.json(ticker)
    });
}

createServer({
    port: 50001
})
