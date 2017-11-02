/**
 * Responds to any HTTP request that can provide a "message" field in the body.
 *
 * @param {!Object} req Cloud Function request context.
 * @param {!Object} res Cloud Function response context.
 */
const PubSub = require('@google-cloud/pubsub');
const pubsub = PubSub();

exports.pubsubPublish = function pubsubPublish(req, res) {
    const topic = pubsub.topic("watanabe-sample");
    const publisher = topic.publisher();
    const dataBuffer = Buffer.from(JSON.stringify(req.body));
    publisher.publish(dataBuffer)
    .then((results) => {
      const messageId = results[0];
      res.status(200).send(`Message ${messageId} published.`).end();
    }).catch((e)=>{
        res.status(500).header({"Content-Type":"application/json"}).json(e).end();    
    });
};
