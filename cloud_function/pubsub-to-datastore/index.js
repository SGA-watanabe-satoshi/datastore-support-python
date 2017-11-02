/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {!Object} event The Cloud Functions event.
 * @param {!Function} The callback function.
 */

//const config = require('./config');
const config = {
  DATASTORE_NAMESPACE: 'watanabe',
  DATASTORE_KIND: 'user_classification',
  KEYWORD_RULE: {'APP_VERSION_1.2': {'version': /1\.2/}, 'APP_VERSION_1.3': {'version': /1\.3/}}
}

const Datastore = require('@google-cloud/datastore');
const datastore = Datastore({namespace:config.DATASTORE_NAMESPACE});

function testObject(actual, expect){
  if(expect instanceof RegExp){
    return expect.test(actual);
  }else{
    for(var prop in expect){
      if(expect[prop] instanceof RegExp){
        try {
          if(actual[prop] == undefined || !expect[prop].test(actual[prop])){
            return false;
          }
        }catch(e){
          return false;
        }
      }else{
        if(!testObject(actual[prop], expect[prop])){
          return false;
        }
      }
    }
    return true;
  }
}

function actionToKeyword(action, keywords){
  var result = [];
  for(var keyword in keywords){
    if(testObject(action, keywords[keyword])){
      result.push(keyword);
    }
  }
  return result;
}

exports.subscribe = function subscribe(event, callback) {
  // The Cloud Pub/Sub Message object.
  const pubsubMessage = event.data;

  // We're just going to log the message to prove that
  // it worked.
  try{
    const action = JSON.parse(Buffer.from(pubsubMessage.data, 'base64').toString());
    const expected = config.KEYWORD_RULE;

    const keywords = actionToKeyword(action, expected);

    if(keywords.length && action.userid){
      const key = datastore.key(config.DATASTORE_KIND);
      const entity = {
          key: key,
          data: [
            {
              name: 'classes',
              value: JSON.stringify(keywords)
            },
            {
              name: 'user_id',
              value: action.userid
            },
            {
              name: 'insert_time',
              value: new Date()
            }
          ]
        };
      datastore.save(entity).then(()=>{
        callback();
      }).catch((e)=>{
        console.log(e);
        callback();
      });
    }else{
      callback();
    }
  }catch(e){
    console.log(e);
    callback();
  }
};
