MonScrape
=========

Scrape responses to a United Way Survey Monkey survey.

Install
-------

::

    $ pip install monscrape-1.0.0.zi

Run
---

First, set up the MONSCRAPE_TOKEN env var::

    $ export MONSCRAPE_TOKEN=0gwc1qdjRCLe6BRKH0fM...kpOtdskQSlIWyim55pgd1fmKx

Then run the download, passing in the collector_id::

    $ monscrape --download 256697438

The download will cache the pages in a new director, ``cache``. You can run the
download command again, and it should not download anything.

After downloading, you can process the pages to get a CSV::

    monscrape --process 256697438 256697438-data.csv

Or, skip the arguments to both download and process.

