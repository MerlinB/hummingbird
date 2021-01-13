'use strict';

var BitcoinP2P = require('bsv-p2p');
require('bsv');
var RPCClient = require('bitcoind-rpc');
var txo = require('txo');
var Queue = require('promise-queue');
var fs = require('fs');
var ReadLastLines = require('read-last-lines');

function _interopDefaultLegacy (e) { return e && typeof e === 'object' && 'default' in e ? e : { 'default': e }; }

var BitcoinP2P__default = /*#__PURE__*/_interopDefaultLegacy(BitcoinP2P);
var RPCClient__default = /*#__PURE__*/_interopDefaultLegacy(RPCClient);
var txo__default = /*#__PURE__*/_interopDefaultLegacy(txo);
var Queue__default = /*#__PURE__*/_interopDefaultLegacy(Queue);
var fs__default = /*#__PURE__*/_interopDefaultLegacy(fs);
var ReadLastLines__default = /*#__PURE__*/_interopDefaultLegacy(ReadLastLines);

function get(file) {
    return new Promise((resolve, reject) => {
        ReadLastLines__default['default'].read(file, 10).then(str => {
            const lines = str.split("\n").filter(line => !!line);
            const last = lines.pop();
            const line = last.split(" ");
            if (line.length !== 5) { throw new Error("expected tape to have 4 elements") }
            if (line[0] === "BLOCK") {
                const height = Number(line[1]);
                resolve(height);
            } else {
                resolve(null);
            }
        }).catch(e => {
            resolve(null);
        });
    });
}

function write(line, file) {
    return new Promise((resolve, reject) => {
        try {
            fs__default['default'].appendFileSync(file, `${line}\n`);
            resolve(true);
        } catch (e) {
            reject(e);
        }
    });
}

const log = require("debug")("hummingbird:helpers");

function sleep(ms) {
    return new Promise(resolve => {
        setTimeout(resolve, ms);
    });
}

const log$1 = require("debug")("hummingbird");


//const messages = new Messages({ Block: bsv.Block, BlockHeader: bsv.BlockHeader, Transaction: bsv.Transaction, MerkleBlock: bsv.MerkleBlock });

const STATE = {
    DISCONNECTED: "DISCONNECTED",
    CONNECTING: "CONNECTING",
    CRAWLING: "CRAWLING",
    LISTENING: "LISTENING",
};

const MODE = {
    BOTH: "BOTH",
    MEMPOOL: "MEMPOOL",
    BLOCK: "BLOCK",
};

class Hummingbird {

    constructor(config={}) {
        this.config = Object.assign({
            "tapefile": "tape.txt",
            "from": 0,
            "state_machines": [],
            "mode": MODE.BOTH,
        }, config);

        if (!this.config.peer || !this.config.peer.host) { throw new Error(`expected peer.host in config`) }
        if (!this.config.tapefile) { throw new Error(`expected tapefile in config`) }
        if (!Number.isInteger(this.config.from)) { throw new Error(`expected from in config`) }

        this.state_machines = this.config.state_machines.map(state_machine => {
            state_machine.log = log$1.extend(state_machine.constructor.name.toLowerCase());
            return state_machine;
        });

        this.state = STATE.DISCONNECTED;

        this.mode = this.config.mode.toUpperCase();
        if (!MODE[this.mode]) { throw new Error(`unexpected mode ${this.mode}`) }

        this.reconnect = (this.config.reconnect === undefined ? true : false);

        const rpcconfig = Object.assign({}, {
            protocol: "http",
            host: "127.0.0.1",
            port: "8332",
        }, this.config.rpc);

        this.ready = function() {};
        this.blockreq = null;

        this.queue = new Queue__default['default'](1, Infinity);

        this.rpc = new RPCClient__default['default'](rpcconfig);

        this.currheight = 0;
        this.blockheight = 0;

        const stream = true;
        const validate = true;

        this.peer = new BitcoinP2P__default['default']({ node: this.config.peer.host, stream, validate, DEBUG_LOG: false });

        this.peer.on('transactions', async ({ header, finished, transactions }) => {
            if (!header) {
                if (this.mode == MODE.MEMPOOL || this.mode == MODE.BOTH) {
                    if (this.state == STATE.LISTENING) {

                        for (const [index, transaction] of transactions) {
                            const txhash = transaction.buffer.toString("hex");
                            const tx = await txo__default['default'].fromTx(txhash);

                            this.queue.add(() => {
                                return this.ontransaction(Object.assign(tx, {hex: txhash})); // return a promise
                            }).catch(e => {
                                log$1(`error while processing queue`);
                                throw e;
                            });
                        }
                    }
                }
            } else {
                if (this.mode == MODE.MEMPOOL) {
                    throw new Error("NOT HANDLED YET x2");
                } else {
                    if (this.state === STATE.CRAWLING && this.blockreq) {
                        let relindex  = 0;
                        for (const [index, transaction] of transactions) {

                            relindex += 1;

                            const tx = await this.parseTransaction(header, this.blockreq.height, transaction);
                            const isTransactionFinished = (finished && relindex === transactions.length);
                            this.queue.add(() => {
                                return this.ontransaction(tx, isTransactionFinished); // return a promise
                            }).catch(e => {
                                log$1(`error while processing queue`);
                                throw e;
                            });
                        }

                        if (finished) {
                            const timestamp = Math.floor(Date.now() / 1000);
                            const logline = `BLOCK ${this.blockreq.height} ${header.hash.toString("hex")} ${header.prevHash.toString("hex")} ${timestamp}`;
                            await write(logline, this.config.tapefile);

                            const diff = (Date.now() - this.blockreq.start) / 1000;
                            log$1(`fetched block ${this.blockreq.height} in ${diff} seconds`);
                            this.blockreq.resolve();
                            this.blockreq = null;
                        }
                    } else if (this.state == STATE.LISTENING) {
                        this.blockheight = await this.heightforhash(header.hash.toString());

                        for (const [index, transaction] of transactions) {
                            const txhash = transaction.buffer.toString("hex");
                            const tx = await txo__default['default'].fromTx(txhash);

                            this.queue.add(() => {
                                return this.ontransaction(Object.assign(tx, {hex: txhash})); // return a promise
                            }).catch(e => {
                                log$1(`error while processing queue`);
                                throw e;
                            });
                        }
                    }
                }
            }
        });

        this.peer.on('disconnected', async ({ disconnects }) => {
            await this.ondisconnect();
        });

        this.peer.on('connected', () => {
            this.onconnect();
        });

        if (this.mode == MODE.MEMPOOL) {
            log$1(`setup hummingbird in mempool`);
        } else if (this.mode == MODE.BLOCK) {
            log$1(`setup hummingbird in block`);
        }  else {
            log$1(`setup hummingbird`);
        }
    }

    // ACTIONS

    async start() {
        log$1(`start`);

        for (const state_machine of this.state_machines) {
            if (state_machine.onstart) {
                await state_machine.onstart();
            }
        }

        await this.onstart();

        await this.connect();
    }

    connect() {
        log$1(`connect`);
        this.state = STATE.CONNECTING;

        this.peer.connect();
    }

    listen() {
        this.state = STATE.LISTENING;

        if (this.mode == MODE.MEMPOOL) {
            this.peer.listenForTxs();
        } else if (this.mode == MODE.BLOCK) {
            this.peer.listenForBlocks();
        } else {
            this.peer.listenForBlocks();
            this.peer.listenForTxs();
        }
        log$1(`listening`);
    }

    async crawl() {
        log$1(`crawling`);

        if (await this.isuptodate()) {
            await this.onrealtime();
            this.listen();
        } else {
            this.state = STATE.CRAWLING;

            while (true) {
                if (await this.isuptodate()) {
                    await this.onrealtime();
                    break;
                }

                await this.crawlblock(await this.curr() + 1);

                //log("waiting");
                await sleep(250);
            }

            this.listen();
        }
    }

    disconnect() {
        log$1(`disconnecting`);
        try {
            delete this.peer.promises.connect;
            this.peer.disconnect();
        } catch (e) {}
    }

    fetchmempool() {
        if (this.peer.connected) {
            log$1(`fetching mempool`);
            this.peer.getMempool();
        } else {
            log$1(`not connected, skipping fetching mempool`);
        }
    }

    async crawlblock(height) {
        return new Promise(async (resolve, reject) => {
            if (!this.peer.connected) {
                log$1(`cannot crawl block ${height}, not connected`);
                resolve();
                return;
            }

            if (this.blockheight > 0) {
                log$1(`handling block ${height} (${this.blockheight - height} behind)`);
            } else {
                log$1(`handling block ${height}`);
            }

            await this.fetch(height);

            resolve();
        });
    }

    async wait() {
        const curr = await this.curr();
        const height = await this.height();
        if (curr === height) {
            await this.onrealtime();
            this.listen();
        } else {
            log$1(`waiting for realtime ${height-curr} behind ${height}`);
            setTimeout(this.wait.bind(this), 10000);
        }
    }

    // EVENTS

    async onconnect() {
        log$1(`on connect`);
        this.ready();

        if (this.mode == MODE.MEMPOOL) {
            this.wait();
        } else {
            this.crawl();
        }
    }

    async ondisconnect() {
        log$1(`on disconnect`);
        this.state = STATE.DISCONNECTED;
        if (this.reconnect) {
            log$1(`reconnecting`);
            this.connect();
        }
    }

    async ontransaction(tx, finished=false) {
        for (const state_machine of this.state_machines) {
            if (!await state_machine.ontransaction(tx, finished)) {
                throw new Error("error processing ontransaction tx");
            }
        }
    }

    async onstart() { }
    async onrealtime() {
        log$1(`realtime`);
        for (const state_machine of this.state_machines) {
            if (state_machine.onrealtime) {
                await state_machine.onrealtime();
            }
        }

        if (this.mode == MODE.MEMPOOL || this.mode == MODE.BOTH) {
            await this.fetchmempool();
        }
    }

    // HELPERS

    async curr() {
        let height = await get(this.config.tapefile);
        if (!height) {
            height = this.config.from - 1;
        }

        this.currheight = height;
        return this.currheight;
    }

    async height() {
        return new Promise((resolve, reject) => {
            this.rpc.getBlockchainInfo(async (err, res) => {
                if (err) { reject(err); }
                else {
                    this.blockheight = res.result.blocks;
                    resolve(this.blockheight);
                }
            });
        });
    }

    async getblock(hash) {
        return new Promise((resolve, reject) => {
            this.rpc.getBlock(hash, async (err, res) => {
                if (err) { reject(err);
                } else {
                    resolve(res.result);
                }
            });
        });
    }

    async heightforhash(hash) {
        return new Promise(async (resolve, reject) => {
            this.rpc.getBlockHeader(hash, async (err, res) => {
                if (err) { throw new Error(`error while fetching height for hash ${hash} ${err}`) }
                resolve(res.result.height);
            });
        });
    }

    async hashforheight(height) {
        return new Promise(async (resolve, reject) => {
            this.rpc.getBlockHash(height, async (err, res) => {
                if (err) { throw new Error(`error while fetching hash for height ${height} ${err}`) }
                resolve(res.result);
            });
        });
    }

    async isuptodate() {
        return new Promise(async (resolve, reject) => {
            const curr = await this.curr();
            if (curr === (this.config.from-1)) {
                resolve(false);
            } else {
                const height = await this.height();
                resolve(curr >= height);
            }
        });
    }

    fetch(height) {
        log$1(`fetching block ${height}`);
        return new Promise(async (resolve, reject) => {
            if (this.blockreq) {
                reject("block fetch can only be called one at a time");
            } else {
                this.state = STATE.CRAWLING;
                const hash = await this.hashforheight(height);
                this.blockreq = { resolve, reject, height, start: Date.now() };
                this.peer.getBlock(hash);
            }
        });
    }

    async parseTransaction(header, height, transaction) {
        const txhash = transaction.buffer.toString("hex");
        return Object.assign(await txo__default['default'].fromTx(txhash), {
            blk: {
                i: height,
                h: header.hash.toString("hex"),
                t: header.time,
            },
            hex: txhash
        });
    }


    async parseBlock(block, height, transactions) {
        const header = Object.assign( block, { height });
        const txs = await Promise.all(transactions.map(async (tx) => {
            const [index, transaction] = tx;
            const txhash = transaction.buffer.toString("hex");
            return Object.assign(await txo__default['default'].fromTx(txhash), {
                blk: {
                    i: header.height,
                    h: Buffer.from(header.hash, "hex").toString(),
                    t: header.time,
                },
                hex: txhash
            });
        }));

        return { header, txs };
    }

}

Hummingbird.STATE = STATE;
Hummingbird.MODE = MODE;

module.exports = Hummingbird;
