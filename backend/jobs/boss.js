const { PgBoss } = require('pg-boss');

const connectionString = `postgresql://${process.env.PGBOSS_USER}:${process.env.PGBOSS_PASSWORD}@${process.env.PGBOSS_HOST}:${process.env.PGBOSS_PORT || 5432}/${process.env.PGBOSS_DATABASE}`;

const boss = new PgBoss(connectionString);

boss.on('error', error => console.error('[pg-boss] Error:', error));

module.exports = { boss };
