/*jslint node: true */
/*jshint esversion: 6 */
'use strict';

const commands = require('./commands');

/**
 * Basic AWS Lambda handler.
 * @param event Message from Telegram
 * @param context Lambda context
 * @returns HTML status response with statusCode 200
 */
exports.handler = (event, context) => {
    console.log('starting to process message');

    const standardResponse = {
        statusCode: 200,
    };
    var command = event.command;

    commands.process(command, event).then((result) => {
        context.succeed(result);
    }).catch((e) => {
        console.log('Error while loading');
        console.log(e);
        context.fail(e);
    });

    return standardResponse;
};