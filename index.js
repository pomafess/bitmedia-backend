const sqlite3 = require('sqlite3').verbose();
const express = require('express');
const fs = require('fs');
const app = express();
const cors = require('cors');
const PORT = process.env.PORT || 3000;

const db = new sqlite3.Database('./test.db', (err) => {
    if (err) {
        console.error(err.message);
        process.exit(1);
    }
    console.log('Connected to the SQlite database.');
});

db.serialize(() => {
    db.run('CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT,' +
        'email TEXT, gender TEXT, ip_address TEXT)', function (err) {
            if (err) {
                return console.log(err.message);
            }
            console.log('Table users created');
        });
    db.get('SELECT COUNT(*) FROM users', (err, row) => {
        if (err) {
            return console.error(err.message);
        }
        const count = row['COUNT(*)'];
        console.log('Rows in users table: ' + count);
        if (!count) {
            console.log('Starting import from file users.json')
            const rawdata = fs.readFileSync('users.json');
            const data = JSON.parse(rawdata);
            data.forEach((line) => {
                db.run('INSERT into users (id, first_name, last_name, email, gender, ip_address) VALUES (?, ?, ?, ?, ?, ?)',
                    [line.id, line.first_name, line.last_name, line.email, line.gender, line.ip_address],
                    function (err) {
                        if (err) {
                            return console.log(err.message);
                        }
                        //console.log(`A row has been inserted to users with rowid ${this.lastID}`);
                    }
                )
            })
            console.log('Import of users complete');
        }
    });
})

db.serialize(() => {
    db.run('CREATE TABLE IF NOT EXISTS users_statistic (user_id INTEGER, date TEXT, page_views INTEGER, clicks INTEGER)', function (err) {
        if (err) {
            return console.log(err.message);
        }
        console.log('Table user_statistic created');
    });
    db.get('SELECT COUNT(*) FROM users', (err, row) => {
        if (err) {
            return console.error(err.message);
        }
        count = row['COUNT(*)'];
        console.log('Rows in users_statistic table: ' + count);
        if (!count) {
            console.log('Starting import from file users_statistic.json')
            const rawdata = fs.readFileSync('users_statistic.json');
            const data = JSON.parse(rawdata);
            data.forEach((line) => {
                db.run('INSERT into users_statistic (user_id, date, page_views, clicks) VALUES (?, ?, ?, ?)',
                    [line.user_id, line.date, line.page_views, line.clicks],
                    function (err) {
                        if (err) {
                            return console.log(err.message);
                        }
                        //console.log(`A row has been inserted to users_statistic with rowid ${this.lastID}`);
                    }
                )
            })
            console.log('Import of users_statistic complete');
        }
    });
})

app.use(cors())

app.get('/users', function (req, res) {
    const size = eval(req.query.size);
    if (isNaN(size) || size < 1) {
        throw new Error('size - incorrect parameter');
    }
    const page = eval(req.query.page);
    if (isNaN(page) || page < 1) {
        throw new Error('page - incorrect parameter');
    }
    const start = (page - 1) * size + 1;
    const end = size * page;
    const sql = 'SELECT users.* , SUM(users_statistic.page_views) AS total_page_views, SUM(clicks) AS total_clicks FROM users ' +
        'LEFT JOIN users_statistic ' +
        'ON users_statistic.user_id=users.id ' +
        'WHERE users.id >= ? AND users.id <= ? ' +
        'GROUP BY users.id'
    db.serialize(() => {
        db.all(sql, [start, end], (err, rows) => {
            if (err) {
                throw err;
            }
            res.send(rows)
        })
    }

    )
})

app.get('/stats/', function (req, res) {
    const id = eval(req.query.id);
    if (isNaN(id)) {
        throw new Error('id should be integer');
    }
    const from = req.query.from || "1970-01-01";
    const dateFrom = new Date(from);
    if (!isNaN(from) || isNaN(dateFrom)) {
        throw new Error('from - incorrect date format, expected yyyy-mm-dd');
    }

    const to = req.query.to || "2200-01-01";
    const dateTo = new Date(to);
    if (!isNaN(to) || isNaN(dateTo)) {
        throw new Error('to - incorrect date format, expected yyyy-mm-dd');
    }
    const sql = 'SELECT * FROM users_statistic WHERE user_id=? AND date>=? AND date <=?';

    db.serialize(() => {
        db.all(sql, [id, dateFrom.toISOString().split('T')[0], dateTo.toISOString().split('T')[0]], (err, rows) => {
            if (err) {
                throw err;
            }
            res.send(rows)
        })
    }

    )
})

app.use((err, request, response, next) => {
    console.log(err)
    response.status(500).send('Error: ' + err.message)
})

app.listen(PORT, () => { console.log(`App listening at http://localhost:3000`) })