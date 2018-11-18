docker_env:
  username: ${DB_USERNAME}
  adapter: mysql2
  database: workflow_db
  host: ${DB_HOST}
  port: ${DB_PORT}
  retries: 2
  password: ${DB_PASSWORD}
  encoding: utf8
  reconnect: true