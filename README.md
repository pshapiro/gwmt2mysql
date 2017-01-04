Google Search Queries API to MySQL Database
===========================================

This script allows you to download Google Search Query data to a MySQL database as shown in the [Search Wilderness Blog by Paul Shapiro][1]

# Prerequisites
* You already have a [Google Search Console][2] account setup with your profiles verified.

# Dependencies
* [Python 2.7][3]
* [Google API Python Client][4]
* [CRON][5] (if scheduling)
* MySQL database
* MySQLdb Python library

# Setup
- Setup a blank MySQL database and insert the credentials into the script
- Connect to your Google Search Console account:
    - Visit [Google Developer Console][6]
    - Create a new Project (it takes a few seconds to load)
    - Click Enable API and search for "Google Search Console API". Click the name, then Enable.
    - Click Credentials
    - Click OAuth Consent Screen and fill in your details
    - Click Create Credentials > OAuth Client ID > Other
    - Look for the new client ID and click the download button (down arrow)
    - Rename the file to `client_secrets.json`
    - Move it to the same directory as the `searchanalytics2mysql` script

# Scheduling

You can use CRON to setup your script as a background task for recurring events.
Download analytics data on the 1st of each month.

```
sudo crontab -e
0 0 1 * * /{path to your script}/searchanalytics2mysql
```
Press `Ctrl + X` then `Y` to save and exit the CRON file.

# Help

If your `searchanalytics2mysql` file is not running, you may need to change the permissions to allow execution.

```
chmod +x ./searchanalytics2mysql
```

# Change Log
- 2016/11/04: Improve README, script helpers and make an executable file
- 2015/8/5: Updated script to work with the new Search Analytics API

Copyright (c) 2016 [Paul Shapiro][7] and [contributors][8], released under the [Apache 2.0 License][9]

[1]: http://searchwilderness.com/gwmt-data-python/
[2]: https://www.google.com/webmasters/tools/home?hl=en
[3]: https://www.python.org/download/releases/2.7/
[4]: https://github.com/google/google-api-python-client
[5]: http://www.unixgeeks.org/security/newbie/unix/cron-1.html
[6]: https://console.developers.google.com/apis/dashboard
[7]: https://github.com/pshapiro
[8]: https://github.com/pshapiro/gwmt2mysql/graphs/contributors
[9]: http://www.apache.org/licenses/LICENSE-2.0
