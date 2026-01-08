const { PgBoss } = require('pg-boss');

const connectionString = `postgresql://${process.env.DATABASE_USER}:${process.env.DATABASE_PASSWORD}@${process.env.DATABASE_HOST}:${process.env.DATABASE_PORT || 5432}/${process.env.DATABASE_NAME}`;

const boss = new PgBoss(connectionString);

boss.on('error', error => console.error('[pg-boss] Error:', error));

module.exports = { boss };
