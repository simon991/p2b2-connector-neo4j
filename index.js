'use strict';

const Web3 = require("web3");
var Promise = require("es6-promise").Promise;
const winston = require('winston');
var neo4j = require('neo4j-driver').v1;
var web3 = new Web3(new Web3.providers.HttpProvider("http://localhost:8545"));

// At RisingStack, we usually set the configuration from an environment variable called LOG_LEVEL
// winston.level = process.env.LOG_LEVEL
winston.level = 'info';

const username = "neo4j";
const password = "p2b2";
const uri = "bolt://localhost:7687";
var session = null;
var driver = null;
var startBlock = -1;
// var startBlock = 238687;

var isFunction = (f) => {
    return (typeof f === 'function');
};

var Neo4jConnector = function () {
};

Neo4jConnector.prototype.connect = () => {
    return new Promise((resolve, reject) => {
        var newDriver = neo4j.driver(uri, neo4j.auth.basic(username, password));
        let newSession = newDriver.session();

        if (newSession._open != true) {
            winston.log('error', 'Neo4jConnector - Driver instantiation failed');
            reject('Driver instantiation failed');
        } else {
            winston.log('info', 'Neo4jConnector - Driver instantiation succeeded');
            driver = newDriver;
            session = newSession;
            resolve(true);
        }

        // TODO: The approach below would be better, but for some reason it does not call the callbacks
        // Register a callback to know if driver creation was successful:
        /*newDriver.onCompleted = () => {
         driver = newDriver;
         session = newSession;
         resolve(newSession);
         };*/
        // Register a callback to know if driver creation failed.
        // This could happen due to wrong credentials or database unavailability:
        /*newDriver.onError = (error) => {
         reject(error);
         };*/
    })
};

Neo4jConnector.prototype.disconnect = () => {
    session.close();
    driver.close();
};

Neo4jConnector.prototype.getLastBlock = (callback) => {
    if (!isFunction(callback)) {
        throw new Error("missing callback function parameter")
    } else {
        let resultPromise = session.run('MATCH (n:Block) return MAX(n.blockNumber)');

        resultPromise.then(result => {
            let singleRecord = result.records[0];
            let singleResult = singleRecord.get(0);
            let lastBlock = startBlock;
            if (singleResult) lastBlock = singleResult.low;

            winston.log('debug', 'Neo4jConnector - Last inserted block:', {
                block: lastBlock
            });

            callback(null, lastBlock);
        }).catch(err => {
            winston.log('error', 'Neo4jConnector - Could not get last inserted block:', {
                error: err.message
            });
            callback(err, null);
        });
    }
};

let createScheme = () => {

    session
        .run('CREATE CONSTRAINT ON (account:Account) ASSERT account.address IS UNIQUE')
        .then((result) => {
            winston.log('debug', 'Database index/constraint for account created');
        })
        .catch((error) => {
            winston.log('error', 'Neo4jConnector - Could not create database scheme', {
                error: err.message
            });
        });

    session
        .run('CREATE CONSTRAINT ON (block:Block) ASSERT block.blockNumber IS UNIQUE')
        .then((result) => {
            winston.log('debug', 'Database index/constraint for block created');
        })
        .catch((error) => {
            winston.log('error', 'Neo4jConnector - Could not create database scheme', {
                error: err.message
            });
        });
};

Neo4jConnector.prototype.insert = (block, callback) => {
    if (!isFunction(callback)) {
        throw new Error("missing callback function parameter")
    } else {
        // if lastBlock == -1 create database scheme (uniqueness/indexes of accounts and blocks etc.)
        // is done asynchronous (we do not wait for the scheme to be created)
        if (block.number === startBlock + 1) {
            createScheme();
        }

        // variable to decide if the transaction should be committed or rolled back
        let success = true;
        // the end result of the transaction
        let transactionResult = null;
        // Create a transaction to run multiple statements
        let tx = session.beginTransaction();
        // A little pointer trick to use the same array in all promise scopes and avoid double check or duplicate creation
        let checkedAccounts = {accounts: []};

        /*********************** inserting block as a node and chaining them with edges **************/
        createBlocks(tx, block, (err, res) => {
            if (res) transactionResult = res; else success = false;
            /*********************** chaining blocks with edges **************/
            chainBlocks(tx, block, checkedAccounts, (err, res) => {
                if (err) success = false;
                if (block.transactions.length === 0) {
                    // This will commit the transaction because it did not contain any blocks
                    commitTransaction(tx, success, block, (err, res) => {
                        if (res) {
                            if (block.number % 1000 === 0) {
                                winston.log('info', 'Neo4jConnector - inserted the next 1000 blocks | last block:', block.number);
                            }
                            callback(null, transactionResult);
                        } else callback(err, null);
                    });
                } else {
                    /*********************** Inserting account/contract nodes **********/

                        // First we need to create all contracts, since we have to wait for the transaction receipt.
                        // Otherwise we potentially violate the uniqueness constraint. That could happen when in one
                        // transaction the account is created and in the next transaction of the block it is called immediately.
                    let contractPromises = [];
                    let transactionsNoContract = [];
                    for (let transaction of block.transactions) {
                        if (transaction.to === null) {
                            // the TO address is null on contract creation
                            // so this handles all transactions with account creation ...
                            contractPromises.push(insertAccounts(tx, transaction, block, checkedAccounts));
                        } else {
                            // ... and this handles all transactions that are no contract creations
                            transactionsNoContract.push(transaction);
                        }
                    }

                    if (contractPromises.length === 0) contractPromises.push(insertAccounts(null, null, null, null));

                    // After we created the contracts, we can create the external accounts.
                    Promise.all(contractPromises).then(() => {


                        // Iterate over the transactions, that are in the block and create accounts if not already done
                        let externalAccountPromises = [];
                        for (let transaction of transactionsNoContract) {
                            externalAccountPromises.push(insertAccounts(tx, transaction, block, checkedAccounts));
                        }
                        if (externalAccountPromises.length === 0) externalAccountPromises.push(insertAccounts(null, null, null, null));
                        Promise.all(externalAccountPromises).then(() => {
                            /*********************** Inserting transactions as edges between accounts/contracts. **********/

                                // Iterate over the transactions, that are in the block and create accounts if not already done
                            let transactionPromises = [];
                            for (let transaction of block.transactions) {
                                transactionPromises.push(insertTransaction(tx, transaction, checkedAccounts));
                            }




                            /*********************** Inserting transactions as edges between accounts/contracts. **********/
                            Promise.all(transactionPromises).then(() => {
                                // This will commit the transaction
                                commitTransaction(tx, success, block, (err, res) => {
                                    if (res) {
                                        if (block.number % 1000 === 0) {
                                            winston.log('info', 'Neo4jConnector - inserted the next 1000 blocks | last block:', block.number);
                                        }
                                        callback(null, transactionResult);
                                    } else callback(err, null);
                                });
                            }).catch(err => {
                                // This will roll back the transaction
                                success = false;
                                commitTransaction(tx, success, block, (err, res) => {
                                    if (res) {
                                        callback(null, transactionResult);
                                    } else callback(err, null);
                                });
                            });
                        }).catch(err => {
                            // This will roll back the transaction
                            success = false;
                            commitTransaction(tx, success, block, (err, res) => {
                                if (res) callback(null, transactionResult); else callback(err, null);
                            });
                        });
                    }).catch(err => {
                        // This will roll back the transaction
                        success = false;
                        commitTransaction(tx, success, block, (err, res) => {
                            if (res) callback(null, transactionResult); else callback(err, null);
                        });
                    });
                }
            });
        });
    }
};

/****************************************************************
 *** Start: Functions used to insert the blocks in the graph. ***
 ****************************************************************/

/**
 * Creates a Block as a node in the graph database
 * @param tx
 * @param block
 * @param callback
 */
var createBlocks = (tx, block, callback) => {
    let transactionResult = null;
    // create a Block node
    // run statement in a transaction
    let queryCreateBlock = 'CREATE (b:Block {blockNumber: $blockNumber, difficulty: $blockDifficulty, extraData: $blockExtraData, ' +
        'gasLimit: $blockGasLimit, gasUsed: $blockGasUsed, miner: $blockMiner, size: $blockSize, ' +
        'timestamp: $blockTimestamp, totalDifficulty: $blockTotalDifficulty}) RETURN b LIMIT 1';
    let paramsCreateBlock = {
        blockNumber: neo4j.int(block.number),
        blockDifficulty: neo4j.int(block.difficulty),
        blockExtraData: block.extraData,
        blockGasLimit: neo4j.int(block.gasLimit),
        blockGasUsed: neo4j.int(block.gasUsed),
        blockMiner: block.miner,
        blockSize: neo4j.int(block.size),
        blockTimestamp: neo4j.int(block.timestamp),
        blockTotalDifficulty: neo4j.int(block.totalDifficulty)
    };
    tx.run(queryCreateBlock, paramsCreateBlock)
        .subscribe({
            onNext: (record) => {
                transactionResult = record;
                winston.log('debug', 'Neo4jConnector - New block node created', {
                    // node: record
                });
                callback(null, transactionResult);
            },
            onCompleted: () => {
                //   session.close();
            },
            onError: (error) => {
                winston.log('error', 'Neo4jConnector - Transaction statement failed:', {
                    error: error.message
                });
                callback(error, null);
            }
        });
};

/**
 * Creates edges from one block-node the its predecessor. Furthermore, the miner account is created if not
 * already done, and an edge from miner to block is created.
 * @param tx
 * @param block
 * @param callback
 */
var chainBlocks = (tx, block, checkedAccounts, callback) => {
    checkAccountExistence(tx, block.miner, checkedAccounts, (err, res) => {
        let accountsArray = [];
        if (err) {
            callback(err, null);
        } else {
            if (res === false) accountsArray.push({address: block.miner, contract: false});
            /*********************** create the miner account if not existing **************/
            createAccounts(tx, accountsArray, (err, res) => {
                if (err) callback(err, null); else {
                    // create an edge from miner to the block
                    let queryCreateMinerEdge = 'MATCH (bNew:Block {blockNumber: $blockNumber}), ' +
                        '(m:Account {address: $minerAddress}) ' +
                        'CREATE (m)-[mined:Mined ]->(bNew) RETURN mined LIMIT 1 ';
                    let paramsCreateMinerEdge = {
                        blockNumber: neo4j.int(block.number),
                        minerAddress: block.miner
                    };
                    tx.run(queryCreateMinerEdge, paramsCreateMinerEdge)
                        .subscribe({
                            onNext: (record) => {
                                winston.log('debug', 'Neo4jConnector - New edge from miner to block inserted', {
                                    //edge: record
                                });

                                /*********************** create the transaction edge **************/
                                if (block.number === (startBlock + 1)) {
                                    callback(null, true);
                                } else {
                                    // chain the Block nodes with edges
                                    let queryCreateBlockEdge = 'MATCH (bNew:Block {blockNumber: $blockNumber}), (bOld:Block {blockNumber: $previousBlockNumber}) ' +
                                        'CREATE (bOld)-[c:Chain ]->(bNew) RETURN c LIMIT 1 ';
                                    let paramsCreateBlockEdge = {
                                        blockNumber: neo4j.int(block.number),
                                        previousBlockNumber: neo4j.int(block.number - 1)
                                    };
                                    tx.run(queryCreateBlockEdge, paramsCreateBlockEdge)
                                        .subscribe({
                                            onNext: (record) => {
                                                winston.log('debug', 'Neo4jConnector - New block chained to the last one', {
                                                    //edge: record
                                                });
                                                callback(null, record);
                                            },
                                            onError: (error) => {
                                                winston.log('error', 'Neo4jConnector - Transaction statement failed:', {
                                                    error: error.message
                                                });
                                                callback(error, null);
                                            }
                                        });
                                }


                            },
                            onError: (error) => {
                                winston.log('error', 'Neo4jConnector - Transaction statement failed:', {
                                    error: error.message
                                });
                                callback(error, null);
                            }
                        });


                }
            });
        }
    });


};

var insertAccounts = (tx, transaction, block, checkedAccounts) => {
    /*********** If the accounts/contracts are not created as nodes yet, we have to do it here ************/
    return new Promise((resolve, reject) => {
        if(tx === null && transaction === null && block === null && checkedAccounts === null) resolve(true); else {

        // check if the sending and receiving account/contract are already created as nodes in the graph. If not create them.

        // This array contains the accounts that need to be created, because so far they do no exist in the graph
        let accountsArray = [];
        /*********************** Checks if the FROM account exists **************/
        checkAccountExistence(tx, transaction.from, checkedAccounts, (err, res) => {
            if (err) reject(err); else {
                if (res === false) accountsArray.push({address: transaction.from, contract: false});
                /*********************** Checks if the TO account exists **************/
                checkAccountExistence(tx, transaction.to, checkedAccounts, (err, res) => {
                    if (err) reject(err); else {
                        let toAccountIsContract = false;
                        if (transaction.to !== null) {
                            // the TO address is not null, thus no contract creation
                            if (res === false) accountsArray.push({
                                address: transaction.to,
                                contract: toAccountIsContract
                            });
                            /*********************** create the accounts if not existing **************/
                            createAccounts(tx, accountsArray, (err, res) => {
                                if (err) reject(err); else {
                                    resolve(res);
                                }
                            });

                        } else {
                            toAccountIsContract = true;
                            if (web3.isConnected()) {
                                let fullTransaction = web3.eth.getTransactionFromBlock(transaction.blockNumber, transaction.transactionIndex);
                                let transactionReceipt = web3.eth.getTransactionReceipt(fullTransaction.hash);

                                // I am checking this here, because sometimes a transaction is sent to an address
                                // where later a contract is created (is this a hash collision????? Because prior
                                // to the contract creation you do not know its address...)
                                checkAccountExistence(tx, transactionReceipt.contractAddress, checkedAccounts, (err, res) => {
                                    if (err) reject(err); else {

                                        for (let transactionPointer of block.transactions) {
                                            if (transactionPointer.transactionIndex === transaction.transactionIndex) {
                                                transactionPointer.to = transactionReceipt.contractAddress;
                                                checkedAccounts.accounts.push(transactionReceipt.contractAddress);
                                            }
                                        }

                                        if (res === false) {

                                            accountsArray.push({
                                                address: transactionReceipt.contractAddress,
                                                contract: toAccountIsContract
                                            });

                                            /*********************** create the accounts if not existing **************/
                                            createAccounts(tx, accountsArray, (err, res) => {
                                                if (err) reject(err); else {

                                                    resolve(res);
                                                }
                                            });

                                        } else {
                                            // In this case, prior to contract creation, there was an transaction to
                                            // the account address. So it was created as external account before and
                                            // we now need to change the labels. (e.g. 0x8d2fbac64126f58394d9162a5c9270ca37cc0fed or 0x332b656504f4EAbB44C8617A42AF37461a34e9dC)
                                            changeAccountFromExternalToContract(transactionReceipt.contractAddress, tx, (err, res) => {
                                                if (err) reject(err); else resolve(res);
                                            })
                                        }
                                    }
                                });
                            } else {
                                winston.log('error', 'Neo4jConnector - web3 is not connected to your ethereum node!');
                                reject(new Error());
                            }
                        }
                    }
                });
            }
        });}
    })
};

let changeAccountFromExternalToContract = (accountAddress, tx, callback) => {
    // in this case a transaction was sent to the address where later an account is created
    // change the accounts label from external to contract
    let queryChangeLabel = 'MATCH (n:Account) ' +
        'WHERE n.address = $address ' +
        'REMOVE n:External ' +
        'SET n:Contract ' +
        'RETURN n LIMIT 1 ';
    let paramsChangeLabel = {
        address: accountAddress,
    };

    tx.run(queryChangeLabel, paramsChangeLabel)
        .subscribe({
            onNext: (record) => {
                winston.log('warn', 'Neo4jConnector - Account changed from external to contract', {
                    edge: record
                });
                callback(null, true);
            },
            onError: (error) => {
                winston.log('error', 'Neo4jConnector - Account change from external to contract failed:', {
                    error: error.message
                });
                callback(error, null);
            }
        });
};


var checkAccountExistence = (tx, accountAddress, checkedAccounts, callback) => {
    if (checkedAccounts.accounts.indexOf(accountAddress) !== -1) {
        callback(null, true);
    } else {
        checkedAccounts.accounts.push(accountAddress);
        tx.run("MATCH (a:Account) WHERE a.address = $address " +
            "RETURN count(a)", {address: accountAddress})
            .subscribe({
                onNext: (record) => {
                    if (record.get(0).low === 0) {
                        // from account needs to be created
                        //winston.log('debug', 'Neo4jConnector - Account existance check done:', {exists: false, address:accountAddress});
                        callback(null, false);
                    } else if (record.get(0).low === 1) {
                        // account or contract already exists
                        //winston.log('debug', 'Neo4jConnector - Account existance check done:', {exists: true, address:accountAddress});
                        callback(null, true);
                    } else if (record.get(0).low > 1) {
                        // Error: database is corrupted (multiple nodes nodes with same address exist)
                        winston.log('error', 'Neo4jConnector - database is corrupted (multiple nodes nodes with same address exist)');
                        callback(new Error("Database is corrupted (multiple nodes nodes with same address exist)"), null);
                    }
                },
                onError: (error) => {
                    winston.log('error', 'Neo4jConnector - ???:', {
                        error: error.message
                    });
                    callback(error, null);
                }
            });
    }
};

/**
 *
 * @param tx The Neo4j session transaction
 * @param accounts Is expecting an array in the following form:
 *                  [{address: '0x52a31...', contract: false}, {address: '0x2e315...', contract: true}]
 */
var createAccounts = (tx, accounts, callback) => {
    let query;
    let params;
    if (accounts.length === 1) {
        query = "CREATE (a:";
        if (accounts[0].contract) query = query + "Account:Contract"; else query = query + "Account:External";
        query = query + " {address: $address}) RETURN a";
        params = {address: accounts[0].address};
    } else if (accounts.length === 2) {
        query = "CREATE (a:";
        if (accounts[0].contract) query = query + "Account:Contract"; else query = query + "Account:External";
        query = query + " {address: $address}) CREATE (a2:";
        if (accounts[1].contract) query = query + "Account:Contract"; else query = query + "Account:External";
        query = query + " {address: $address2}) RETURN a";
        params = {address: accounts[0].address, address2: accounts[1].address};
    } else if (accounts.length === 0) {
        callback(null, true);
    } else {
        winston.log('error', 'Neo4jConnector - Invalid number of accounts to create!');
        callback(new Error("Invalid number of accounts to create!"), null);
    }

    if (accounts.length === 1 || accounts.length === 2) {
        // create an Account node
        tx.run(query, params).subscribe({
            onNext: (record) => {
                winston.log('debug', 'Neo4jConnector - New account node(s) created', {accounts: accounts});
                callback(null, record);
            },
            onError: (error) => {
                winston.log('error', 'Neo4jConnector - Transaction statement failed', {
                    error: error.message
                });
                callback(error, null);

            }
        });
    }
};

/**
 * Inserts a transaction to the graph database. To do so it first checks if the required accounts/contracts already
 * exist as nodes in the database and if not, creates them. Then it inserts the transaction itself as edges between
 * accounts/contracts.
 * @param tx
 * @param transaction
 * @returns {Promise}
 */
var insertTransaction = (tx, transaction, checkedAccounts) => {
    /*********** If the accounts/contracts are not created as nodes yet, we have to do it here ************/
    return new Promise((resolve, reject) => {
        /*********************** Insert transactions as edges **************/
        // Insert the transactions as edges between the sending and
        // receiving account/contract: (Account) ---transaction ---> (Account)
        // [   Alternatively: (Account) ----out----> (Transaction) ----in----> (Account)   ]
        insertTransactionEdge(tx, transaction, (err, res) => {
            if (err) reject(err); else {
                resolve(transaction);
            }
        });
    })
};

/**
 * Creates edges from one block-node the its predecessor
 * @param tx
 * @param block
 * @param callback
 */
var insertTransactionEdge = (tx, transaction, callback) => {

    // chain the account/contract nodes with edges representing the transaction
    let queryCreateTransactionEdge = 'MATCH (aFrom:Account {address: $fromAddress}), (aTo:Account {address: $toAddress}) ' +
        'CREATE (aFrom)-[t:Transaction { to: $toAddress, from: $fromAddress,  ' +
        'blockNumber: $blockNumber, transactionIndex: $transactionIndex, value: $value, gas: $gas, ' +
        'gasPrice: $gasPrice, input: $input}]->(aTo) RETURN t LIMIT 1 ';
    let paramsCreateTransactionEdge = {
        fromAddress: transaction.from,
        blockNumber: neo4j.int(transaction.blockNumber),
        transactionIndex: neo4j.int(transaction.transactionIndex),
        value: neo4j.int(transaction.value), // value is a web3 BigNumber
        gas: neo4j.int(transaction.gas),
        gasPrice: neo4j.int(transaction.gasPrice), // gas price is a web3 BigNumber
        input: transaction.input,
        toAddress: transaction.to
    };

    tx.run(queryCreateTransactionEdge, paramsCreateTransactionEdge)
        .subscribe({
            onNext: (record) => {
                winston.log('debug', 'Neo4jConnector - New edge between accounts inserted representing a transaction', {
                    //edge: record
                });

                callback(null, record);
            },
            onError: (error) => {
                winston.log('error', 'Neo4jConnector - Transaction statement failed:', {
                    error: error.message
                });
                callback(error, null);
            }
        });

};

/**
 * If the success parameter is true, it commits the transaction. If not, it rolls it back.
 * @param tx
 * @param success
 * @param callback
 */
var commitTransaction = (tx, success, block, callback) => {
    //decide if the transaction should be committed or rolled back
    if (success) {
        tx.commit()
            .subscribe({
                onCompleted: () => {
                    // this transaction is now committed
                    winston.log('debug', 'Neo4jConnector - Transaction is now committed', {
                        //result: transactionResult
                    });
                    callback(null, true);
                },
                onError: (error) => {
                    winston.log('error', 'Neo4jConnector - Transaction commit failed, rollback!', {
                        error: error.message
                    });
                    tx.rollback();
                    callback(error, null);
                }
            });
    } else {
        //transaction is rolled black and nothing is created in the database
        winston.log('error', 'Neo4jConnector - Transaction rolled back', {
            block: block
        });
        tx.rollback();
        callback(new Error("At least one statement of the transaction failed!"), null);
    }
};

/****************************************************************
 **** End: Functions used to insert the blocks in the graph. ****
 ****************************************************************/

module.exports = new Neo4jConnector();