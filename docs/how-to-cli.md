# Command Line

## Environment variables

LineUp currently uses redis as queue backend, you might want to configure the connection string like this:

the `LINEUP_REDIS_URI` environment variable has the syntax: `redis://<dbindex>@<host>:<port>/<password>`

```bash
export LINEUP_REDIS_URI='redis://15@localhost:6379'
```

## An example production setup

```bash
lineup --working-dir=/srv/pipelines downloader run --output=rpush@destination-list-in-redis
```


## Pushing data to a pipeline

You can push multiple times at once

```bash
lineup rss-scraper push {"url": "http://www.some-blog.com"} {"url": "http://www.another.com"}
```

Or just once if you will

```bash
lineup rss-scraper push {"url": "http://www.single.com"}
```
