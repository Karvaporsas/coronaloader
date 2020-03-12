/*jslint node: true */
/*jshint esversion: 6 */
'use strict';

const loadHandler = require('./handlers/loadHandler');

module.exports = {
    process(command, data) {
        switch (command) {
            case 'load':
            case 'autoload':
                return loadHandler.autoLoad();
            case 'update':
                return loadHandler.autoLoad(true);
            default:
                return new Promise((resolve, reject) => {
                    reject('Unknown comand');
                });
        }
    },
};