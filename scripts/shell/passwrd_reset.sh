sudo -i && /
su - {DB_USER}  && /
who && /
{DUMP_BINARY}/psql -p 4550 -d {DB_NAME} -U {DB_USER} -c "ALTER USER {DB_USER} WITH PASSWORD '{DB_USER_PASS}';" && \
echo "Successfull";