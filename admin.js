const { kafka } = require('./client')

async function init() {
    const admin = kafka.admin();
    console.log('Admin Connecting...')
    admin.connect();
    console.log('Admin Connection Success...');

    console.log('Creating Topic [rider-updates]')
    await admin.createTopics({
        topics: [{
            topic: 'rider-updates',
            numPartitions: 2, // east nepal and west nepal
        }]
    });
    console.log('Topic Created Success [rider-updates]')
    console.log('Disconnecting Admin..')
    await admin.disconnect();
}

init();