const express = require('express');
const fs = require('fs');
const bodyParser = require('body-parser');
const app = express();
const PORT = 8000;
const { Pool } = require('pg');
// Middleware to parse JSON request bodies
app.use(express.json());

// Endpoint to handle live events
app.post('/liveEvent', (req, res) => {
    // Check authorization header
    const authHeader = req.headers.authorization;
    if (authHeader !== 'secret') {
        return res.status(401).send('Unauthorized');
    }

    // Save event data to file
    const eventData = req.body;
    fs.appendFile('events.log', JSON.stringify(eventData) + '\n', (err) => {
        if (err) {
            console.error('Error saving event:', err);
            return res.status(500).send('Internal Server Error');
        }
        console.log('Event saved:', eventData);
        res.status(200).send('Event saved successfully');
    });
});

app.use(bodyParser.json());

const pool = new Pool({
    user: 'postgres',
    host: 'localhost',
    database: 'db',
    password: 'postgres',
    port: 5432,
  });

app.get('/userEvents/:userId', async (req, res) => {
    const userId = req.params.userId;

    try {
        const client = await pool.connect();

        const result = await client.query('SELECT * FROM users_revenue WHERE user_id = $1', [userId]);

        client.release();

        if (result.rows.length > 0) {
            res.json(result.rows[0]);
        } else {
            res.status(404).json({ error: 'User not found' });
        }
    } catch (error) {
        console.error('Error retrieving user revenue:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.listen(PORT, () => {
    console.log(`Server listening on port ${PORT}`);
});