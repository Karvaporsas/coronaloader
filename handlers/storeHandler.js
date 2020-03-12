/*jslint node: true */
/*jshint esversion: 6 */
'use strict';

const database = require('../database');
const DEBUG_MODE = process.env.DEBUG_MODE === 'ON';

module.exports = {
    store(cases, isUpdate) {
        if (DEBUG_MODE) {
            console.log('starting to store cases');
        }
        if(!cases) {
            return new Promise((resolve, reject) => {
                resolve({status: 1, message: 'Nothing found'});
            });
        }
        if (isUpdate) {
            return database.updateCases(cases);
        } else {
            return database.insertCases(cases);
        }
    }
};