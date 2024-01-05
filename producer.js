const { kafka } = require("./client")
const readline = require('readline')

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
})
async function init() {
    const producer = kafka.producer();

    console.log('Connecting Producer...')

    await producer.connect();
    console.log('Producer Connected')
    rl.setPrompt('> ')
    rl.prompt(); // give the ridername and location as prompt
    rl.on('line', async function (line) {
        const [riderName, location] = line.split(' ') // like yogesh eastNP (group is locationwise)
        await producer.send({
            topic: 'rider-updates',
            messages: [
                {
                    partition: location.toLowerCase() === 'eastnp' ? 0 : 1,
                    key: 'location-update', value: JSON.stringify({ name: riderName, location })
                }
            ]
        })
    }).on('close', async () => {
        await producer.disconnect();
    })
}
init();