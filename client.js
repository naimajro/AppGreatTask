const fs = require('fs');
const axios = require('axios');

function readEventsFromFile() {
    try {
        const eventsData = fs.readFileSync('events.jsonl', 'utf8');
        const events = eventsData.split('\n').map(line => JSON.parse(line));
        return events;
    } catch (err) {
        console.error('Error reading events from file:', err);
        return [];
    }
}

async function sendEventsToServer(events) {
    const serverUrl = 'http://localhost:8000/liveEvent';
    const secret = 'secret';

    for (const event of events) {
        try {
            const response = await axios.post(serverUrl, event, {
                headers: {
                    'Authorization': secret,
                    'Content-Type': 'application/json'
                }
            });
            console.log('Event sent successfully:', event);
            console.log('Server response:', response.data);
        } catch (error) {
            console.error('Error sending event:', event);
            console.error('Error:', error.response.data);
        }
    }
}

const events = readEventsFromFile();

sendEventsToServer(events);