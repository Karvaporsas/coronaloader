/*jslint node: true */
/*jshint esversion: 6 */
'use strict';

const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const moment = require('moment');
const database = require('../database');
const DEBUG_MODE = process.env.DEBUG_MODE === 'ON';
const THL_CASES_LINK = process.env.THL_CASES_LINK;

module.exports = {
    storeHSOpen(cases, isUpdate) {
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
    },
    storeTHL(cases) {
        return new Promise((resolve, reject) => {
            var dateString = moment().format('YYYY-MM-DD-HH-mm-ss');
            var k = `${dateString}-thl-confirmed.json`;
            var storageParams = {
                Bucket: 'toffel-lambda-charts',
                Key: k,
                Body: JSON.stringify(cases),
                ContentType: 'application/json; charset=utf-8',
            };

            s3.putObject(storageParams, function(err, data) {
                if (err) {
                    console.log('Error storing json data from thl');
                    console.error(err);
                    reject(err);
                } else {
                    database.updateChartLink({
                        chartName: THL_CASES_LINK,
                        url: k
                    }).then(() => {
                        resolve({status: 1, message: 'All good'});
                    }).catch((e) => {
                        reject(e);
                    });


                }
            });
        });
    }
};