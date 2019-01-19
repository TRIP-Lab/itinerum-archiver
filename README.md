# itinerum-archiver
Tool for automatically archiving inactive surveys and displaying a status page



### Getting Started

##### Archiver

Itinerum Archiver finds surveys that do not have a recorded coordinate since the configured `inactivity_date` and exports the data by survey as a PostgreSQL dump, a very similar SQLite version (differences--all non-integer numbers are floats; JSON fields are serialized to TEXT), and as .csv exports.

Itinerum Archiver assumes it is accessing a cloned version of the production database and *will delete data*. The purpose here is to double-check the backup version of the database before any data is removed from a production instance. A master database (`exports.sqlite`)  is created that tracks all exported data and counts of the rows archived.

Itinerum Archiver (`archiver/archiver.py`) is intended to be scheduled as a cronjob to run regularly. When complete, Itinerum Archiver will send an email to notify of any surveys that have been deprecated.



###### Example config.json (to be placed in `./archiver` directory alongside `archiver.py`)

```json
{
    "source_db": {
        "host": "localhost",
        "port": 5432,
        "user": "db_username",
        "password": "db_password",
        "dbname": "db_name"
    },
    "inactivity_date": "2018-06-01T00:00:00Z",
    "output_dir": "./output",
    "receiver_email": {
        "address": "email@example.com"
    },
    "sender_email": {
        "address": "email@example.com",
        "host": "smtp.example.com",
        "port": 587,
        "tls": true,
        "password": "sender_email_password"
    }
}
```

##### WebUI

The `www` directory contains the `status.html` which is a simple table page indicating survey archive statuses:

 - `active` - survey has recently collected data and is running on production
 - `backups created` - survey is inactive and archive files have been generated; survey deleted from clone but still actively exists on production database
 - `archived` - inactive survey has been successfully deleted from production database and archives are available in cold storage

