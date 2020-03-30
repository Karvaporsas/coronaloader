/*jslint node: true */
/*jshint esversion: 6 */
'use strict';

const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const _ = require('underscore');
const moment = require('moment');
const Fs = require('fs');
const Axios = require('axios');
const database = require('./../database');
const DEBUG_MODE = process.env.DEBUG_MODE === 'ON';
const CHART_LINK_DAILY_NEW = process.env.CHART_LINK_DAILY_NEW;
const DAY_PERIOD = 30;

function _loadChartToS3(chartName, body) {
    return new Promise((resolve, reject) => {
        var dateString = moment().format('YYYY-MM-DD-HH-mm-ss');
        const k = `${dateString}${chartName}.png`;
        const url = "https://quickchart.io/chart";
        const path = `/tmp/${k}`;
        const writer = Fs.createWriteStream(path);

        Axios({
            url,
            method: 'POST',
            data: body,
            responseType: 'stream'
        }).then((res) => {
            res.data.pipe(writer);
        }).catch((e) => {
            console.log('Error occured while downloading');
            console.log(e);
            reject(e);
        });

        writer.on('error', function (err) {
            console.log('writer eror');
            console.log(err);
            reject();
        });
        writer.on('finish', function () {
            var imgStorageParams = {
                Bucket: 'toffel-lambda-charts',
                Key: k,
                Body: Fs.createReadStream(path),
                ContentType: 'image/png'
            };

            s3.putObject(imgStorageParams, function(err, data) {
                if (err) {
                    console.error(err);
                } else {
                    resolve({
                        chartName: chartName,
                        url: k
                    });
                }
            });
        });
    });
}

function _createChart(chartName, data) {
    const m = moment().subtract(DAY_PERIOD + 1, 'days').set('hour', 0).set('minutes', 0).set('seconds', 0);
    var casesByDateGroup = _.chain(data)
        .filter(function (c) { return c.date.isAfter(m); })
        .groupBy(function (c) { return c.day; })
        .value();

    var daySlots = [];
    var avgLineValues = [0, 0, 0, 0, 0];
    for (let i = 0; i <= DAY_PERIOD; i++) {
        var dm = moment().subtract(DAY_PERIOD - i, 'days');
        var keyString = dm.format('YYYY-MM-DD');
        var casesByDate = casesByDateGroup[keyString];
        var cases = casesByDate ? casesByDate.length : 0;
        avgLineValues[i % (avgLineValues.length)] = cases;
        var divider = i < (avgLineValues.length -1) ? (i + 1) : avgLineValues.length;
        var avgValue = _.reduce(avgLineValues, function (mem, a) { return mem + a; }, 0) / divider;
        daySlots.push({
            day: keyString,
            dateString: dm.format('D.M.'),
            cases: cases,
            avgValue: avgValue
        });
    }
    var labels = _.map(daySlots, function (slot) { return slot.dateString; });
    var dayValues = _.map(daySlots, function (slot) { return slot.cases; });
    var avgs = _.map(daySlots, function (slot) { return slot.avgValue; });

    var body = {
        backgroundColor: 'rgba(255, 255, 255, 0.5)',
        width: 800,
        height: 600,
        format: 'png',
        chart: {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                        label: 'Uudet tapaukset',
                        data: dayValues,
                        fill: false,
                        borderColor: 'rgba(53,108,181,1)',
                        lineTension: 0.1
                    },{
                        label: 'Trendi',
                        data: avgs,
                        fill: false,
                        borderColor: 'rgba(211, 211, 211, 0.8)',
                        lineTension: 0.3,
                        pointRadius: 0.1
                    }
                ]
            }
        }
    };

    return _loadChartToS3(chartName, body);
}

module.exports = {
    createCharts() {
        if (DEBUG_MODE) {
            console.log('starting to create charts');
        }
        return new Promise((resolve, reject) => {
            database.getConfirmedCases().then((confirmedCases) => {
                return _createChart(CHART_LINK_DAILY_NEW, confirmedCases);
            }).then((chartLink) => {
                return database.updateChartLink(chartLink);
            }).then((status) => {
                resolve({status: 0, message: 'Stuff was done', link: status.link});
            }).catch((e) => {
                reject(e);
            });
        });

    }
};