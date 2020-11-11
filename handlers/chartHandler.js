/*jslint node: true */
/*jshint esversion: 6 */
'use strict';

const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const _ = require('underscore');
const moment = require('moment');
const fs = require('fs');
const Axios = require('axios');
const database = require('./../database');
const utils = require('./../utils');
const DEBUG_MODE = process.env.DEBUG_MODE === 'ON';
const CHART_LINK_DAILY_NEW = process.env.CHART_LINK_DAILY_NEW;
const CHART_LINK_HOSPITALIZATIONS = process.env.CHART_LINK_HOSPITALIZATIONS;
const DAY_PERIOD = 60;
const DATASOURCE_FOR_CHARTS = process.env.DATASOURCE_FOR_CHARTS || 'DB';
const CASE_BUCKET = process.env.CASE_BUCKET;
const REPORTING_AREA = 'Finland';

function _loadChartToS3(chartName, body) {
    return new Promise((resolve, reject) => {
        var dateString = moment().format('YYYY-MM-DD-HH-mm-ss');
        const k = `${dateString}${chartName}.png`;
        const url = "https://quickchart.io/chart";
        const path = `/tmp/${k}`;
        const writer = fs.createWriteStream(path);

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
                Bucket: CASE_BUCKET,
                Key: k,
                Body: fs.createReadStream(path),
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

function _createChart(chartName, data, hcd) {
    const m = moment().subtract(DAY_PERIOD + 1, 'days').set('hour', 0).set('minutes', 0).set('seconds', 0);

    if (hcd && hcd.length) {
        data = _.filter(data, function (c) {return c.healthCareDistrict === hcd; });
    }

    var casesByDateGroup = _.chain(data)
        .filter(function (c) { return c.date.isAfter(m); })
        .groupBy(function (c) { return c.day; })
        .value();

    var daySlots = [];
    var avgLineValues = [0, 0, 0, 0, 0, 0, 0];
    //var daysToDraw = DATASOURCE_FOR_CHARTS == 'S3' ? DAY_PERIOD -1 : DAY_PERIOD;
    var daysToDraw = DAY_PERIOD -1; // not today
    for (let i = 0; i <= daysToDraw; i++) {
        const dm = moment().subtract(DAY_PERIOD - i, 'days');
        const keyString = dm.format('YYYY-MM-DD');
        var cases = casesByDateGroup[keyString] ? casesByDateGroup[keyString].length : 0;

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
        backgroundColor: 'rgba(255, 255, 255, 0.1)',
        width: 1200,
        height: 900,
        format: 'png',
        chart: {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                        label: 'Uudet tapaukset' + (hcd ? ` - ${hcd}` : ''),
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
    var finalChartName = chartName + (hcd ? `_${hcd}` : '');
    console.log(`Final chart name is ${finalChartName}`);

    return _loadChartToS3(finalChartName, body);
}

module.exports = {
    createCharts(hcd, timeout, cached) {
        if (DEBUG_MODE) {
            console.log('starting to create charts');
        }
        return new Promise((resolve, reject) => {
            database.getConfirmedCases(DATASOURCE_FOR_CHARTS, cached).then((confirmedCases) => {

                if (timeout) {
                    return new Promise((resolve, reject) => {
                        setTimeout(function () {
                            console.log(`HCD is ${hcd}`);
                            resolve(_createChart(CHART_LINK_DAILY_NEW, confirmedCases, hcd));
                        }, timeout);
                    });
                } else {
                    console.log(`HCD is ${hcd}`);
                    return _createChart(CHART_LINK_DAILY_NEW, confirmedCases, hcd);
                }
            }).then((chartLink) => {
                return database.updateChartLink(chartLink);
            }).then((status) => {
                resolve({status: 0, message: 'Stuff was done', link: status.link});
            }).catch((e) => {
                reject(e);
            });
        });
    },
    createHospitalizationCharts() {
        return new Promise((resolve, reject) => {
            database.getHospitalizations(DAY_PERIOD + 1).then((hospitalizations) => {
                const mString = moment().subtract(DAY_PERIOD + 1, 'days').set('hour', 0).set('minutes', 0).set('seconds', 0).format(utils.getShortTimeFormat());
                var casesByDateGroup = _.chain(hospitalizations)
                    .filter(function (c) { return c.dateSortString > mString && c.area == REPORTING_AREA; })
                    .groupBy(function (c) { return c.date; })
                    .value();

                var daySlots = [];
                var hasHadValues = false;

                //var maxDeaths = 0;
                for (let i = 0; i <= DAY_PERIOD; i++) {
                    const dm = moment().subtract(DAY_PERIOD - i, 'days');
                    const keyString = dm.format(utils.getShortTimeFormat());
                    const dateGroup = casesByDateGroup[keyString];
                    var inHospital = 0;
                    var inICU = 0;
                    //var deaths = 0;

                    if (dateGroup) {
                        inHospital = _.reduce(casesByDateGroup[keyString], function (mem, c) { return mem + c.totalHospitalised; }, 0);
                        inICU = _.reduce(casesByDateGroup[keyString], function (mem, c) { return mem + c.inIcu; }, 0);
                        //deaths = _.reduce(casesByDateGroup[keyString], function (mem, c) { return mem + c.dead; }, 0);

                        if (inHospital || inICU) {
                            hasHadValues = true;
                        }
                    }

                    //if (deaths > maxDeaths) maxDeaths = deaths;
                    //if (deaths < maxDeaths) deaths = maxDeaths;

                    if (!hasHadValues || (!dateGroup && i == DAY_PERIOD) || !dateGroup) continue;

                    daySlots.push({
                        day: keyString,
                        dateString: dm.format('D.M.'),
                        inHospital: inHospital,
                        inICU: inICU,
                        //deaths: deaths
                    });
                }
                var labels = _.map(daySlots, function (slot) { return slot.dateString; });
                var hospitalValues = _.map(daySlots, function (slot) { return slot.inHospital; });
                var icuValues = _.map(daySlots, function (slot) { return slot.inICU; });
                var deadValues = _.map(daySlots, function (slot) { return slot.deaths; });

                var body = {
                    backgroundColor: 'transparent',
                    width: 1200,
                    height: 900,
                    format: 'png',
                    chart: {
                        type: 'line',
                        data: {
                            labels: labels,
                            datasets: [{
                                    label: 'Sairaalahoidossa',
                                    data: hospitalValues,
                                    fill: false,
                                    borderColor: 'rgba(252, 186, 3, 1)',
                                    pointRadius: 0.1
                                },{
                                    label: 'Tehohoidossa',
                                    data: icuValues,
                                    fill: false,
                                    borderColor: 'rgba(252, 20, 3, 1)',
                                    pointRadius: 0.1
                                }/*,{
                                    label: 'Kuolleet',
                                    data: deadValues,
                                    fill: false,
                                    borderColor: 'rgba(0, 0, 0, 1)',
                                    pointRadius: 0.1
                                }*/
                            ]
                        }
                    }
                };

                return _loadChartToS3(CHART_LINK_HOSPITALIZATIONS, body);
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