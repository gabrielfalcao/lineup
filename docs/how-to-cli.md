# Command Line

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
