const fs = require('fs');
const { Pool } = require('pg');

const pool = new Pool({
    user: 'postgres',
    host: 'localhost',
    database: 'db',
    password: 'postgres',
    port: 5432,
  });

async function processDataProcessor(eventsFilePath) {
    try {
      
        const readStream = fs.createReadStream(eventsFilePath, { encoding: 'utf8' });
        
        let lineNumber = 0; 
        let batchEvents = {}; 
        const batchSize = 1000;


        readStream.on('data', async (chunk) => {
            const lines = chunk.split('\n'); 

           
            for (const line of lines) {
                lineNumber++;
                if (!line.trim()) continue; 

                const event = JSON.parse(line);
               
                if (!batchEvents[event.userId]) {
                    batchEvents[event.userId] = [];
                }
                batchEvents[event.userId].push(event);

                
                if (Object.keys(batchEvents).length >= batchSize) {
                    await processBatch(batchEvents);
                    batchEvents = {}; 
                }
            }
        });

        
        readStream.on('end', async () => {
            
            if (Object.keys(batchEvents).length > 0) {
                await processBatch(batchEvents);
            }
            
            console.log(`Processed all events.`);
        });
    } catch (error) {
        console.error('Error processing events:', error);
    }
}

async function processBatch(batchEvents) {
    let client;
    try {

        client = await pool.connect();
        
        await client.query('BEGIN');

        for (const userId in batchEvents) {
            if (batchEvents.hasOwnProperty(userId)) {
                const events = batchEvents[userId];
                let totalRevenueChange = 0;

                for (const event of events) {
                    if (event.name === 'add_revenue') {
                        totalRevenueChange += event.value;
                    } else if (event.name === 'subtract_revenue') {
                        totalRevenueChange -= event.value;
                    }
                }

                await client.query('INSERT INTO users_revenue (user_id, revenue) VALUES ($1, $2) ON CONFLICT (user_id) DO UPDATE SET revenue = users_revenue.revenue + $2', [userId, totalRevenueChange]);
            }
        }

        await client.query('COMMIT');

        console.log(`Updated revenue for batch of users.`);
    } catch (error) {

        console.error('Error updating user revenue:', error);

        if (client) {
            await client.query('ROLLBACK');
        }
    } finally {

        if (client) {
            client.release();
        }
    }
}

const eventsFilePath = 'events.jsonl';
processDataProcessor(eventsFilePath);
