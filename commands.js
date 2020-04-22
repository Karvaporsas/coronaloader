/*jslint node: true */
/*jshint esversion: 6 */
'use strict';

const loadHandler = require('./handlers/loadHandler');
const chartHandler = require('./handlers/chartHandler');

module.exports = {
    process(command, data) {
        switch (command) {
            case 'load':
            case 'autoload':
                return loadHandler.autoLoad();
            case 'update':
                return loadHandler.autoLoad(true, data.tresholdDays);
            case 'createcharts':
                return chartHandler.createCharts();
            case 'createhospitalization':
                return chartHandler.createHospitalizationCharts();
            default:
                return new Promise((resolve, reject) => {
                    reject('Unknown comand');
                });
        }
    },
};