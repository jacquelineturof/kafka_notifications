const bodyParser = require( 'body-parser' )
const express = require( 'express' )
const cors = require( 'cors' )
const { Kafka } = require('kafkajs')

// currently just one client is supported at a time,
// but can be easily expanded to multiple clients if necessary
const client = {}
const app = express()

app.use( bodyParser.urlencoded({ extended: false }) )
app.use( bodyParser.json() )

const PORT = 3046
const HEADERS =
{
  'Content-Type': 'text/event-stream',
  'Connection': 'keep-alive',
  'Cache-Control': 'no-cache',
  'Access-Control-Allow-Origin': '*'
}

/*
  Kafka setup
*/
const kafka = new Kafka({
  clientId: 'webapp',
  brokers: [
    "broker-4-x2vk6zh7z7x2dfc9.kafka.svc04.eu-de.eventstreams.cloud.ibm.com:9093",
    "broker-2-x2vk6zh7z7x2dfc9.kafka.svc04.eu-de.eventstreams.cloud.ibm.com:9093",
    "broker-0-x2vk6zh7z7x2dfc9.kafka.svc04.eu-de.eventstreams.cloud.ibm.com:9093",
    "broker-5-x2vk6zh7z7x2dfc9.kafka.svc04.eu-de.eventstreams.cloud.ibm.com:9093",
    "broker-3-x2vk6zh7z7x2dfc9.kafka.svc04.eu-de.eventstreams.cloud.ibm.com:9093",
    "broker-1-x2vk6zh7z7x2dfc9.kafka.svc04.eu-de.eventstreams.cloud.ibm.com:9093"
  ],
  ssl: false,
  connectionTimeout: 10_000,
  authenticationTimeout: 10_000,
  sasl: {
    mechanism: 'plain',
    username: 'admin',
    password: 'IGPxFbx4zaSU3HOja0aLRve1eQggVJcu-_V6G3GAQAxr',
  }
})

const consumer = kafka.consumer({ groupId: 'webapp' })


const _sendMessage = message =>
{
  const data = `data: ${ JSON.stringify(message) }\n\n`

  if( !client.response )
  {
    console.warn( `No client connected, message won't be sent: ${ data }` )

    return
  }

  console.log( `Sending ${ data }` )
  client.response.write( data )
}

const handleGetMessages = ( request, response ) =>
{
  console.log( 'Starting connection' )

  response.writeHead( 200, HEADERS )
  client.response = response

  request.on( 'close', () => console.log('Connection closed') )
}

const handleSend = (message) =>
{
  // const message = request.body
  console.log('calling handleSend...')
  console.log('message: ', message)
  if( !message )
    return

  _sendMessage( message )

  // response.end()
}



const kafkaSetup = async () => {
  await consumer.connect()
  await consumer.subscribe({ topic: 'thl-events', fromBeginning: true })
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        value: message.value.toString(),
      })
      const jsonMessage = JSON.parse(message.value.toString())
      console.log('json: ', jsonMessage)
      handleSend(jsonMessage)
    },
  })
}

kafkaSetup()

app.use( cors() )
app.get( '/messages/:userId', handleGetMessages )
app.post( '/send', handleSend )

app.listen( PORT, () =>
  console.log(
    `Notification test server running at http://localhost:${ PORT }`
  )
)
