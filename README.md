# loca3

* Install loca (on external db)
- Install postgres
- Install postgis: sudo apt install postgresql-10-postgis-2.4 postgresql-10-postgis-2.4-scripts
- Create postgres-superuser martin: sudo -u postgres createuser -s martin
- Set passwords to pass for users postgres and martin: 'sudo -u ... psql postgres', \password
- Fix owner/persmissions of /media/martin/loca/... Reqursivly owner to postgres: sudo chown -R postgres:postgres /media/martin/loca/postgresql. And maybe: sudo chmod o=x /media/martin. Test permission: sudo -u postgres ls /media/martin/loca/postgresql
- Change data-dir: Stop postgres (sudo systemctl stop postgresql), edit in /etc/postgresql/10/main/postgresql.conf, start postgres
