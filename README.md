[![Build Status](https://travis-ci.org/gabrielfalcao/lineup.png)](https://travis-ci.org/gabrielfalcao/lineup)
# LineUp - Distributed Pipeline Framework for Python

Lineup is a redis-based
[pipeline](http://en.wikipedia.org/wiki/Pipeline_(software) framework
that turns horizontal scalling seamless.

It's currently providing parallelism through python threads and is
pretty useful for writing systems where the workers make lots of
network I/O.

It scales horizontally, so you can simply run more workers in as many
machines you want.

## Installing

```bash
easy_install curdling
curd install lineup
```

## Philosophy

Lineup focus in:

1. Simplicity: easy to create otherwise complex pipelines
2. Easy-scale: just run more workers and you're good.

## Defining steps

Steps must always implement the method `consume(self, instructions)` and
always call `self.produce()` with it's portion of work.

```python

# myapp/tasks.py

from lineup import Step

import requests

class Scraper(Step):
    def consume(self):
        url = instructions['url']
        response = requests.get(url)

        # pretend you generated a
        # list of things or general
        # metadata

        results = [
            'https://2.gravatar.com/avatar/666e2844d622f8714e8ccf8ffb48a47c'
            'https://1.gravatar.com/avatar/b9aa05d9efc6a3c8eda50f7763ad0715'
            'https://0.gravatar.com/avatar/605d445205b61ec11185a28dc4ab9323'
            'https://0.gravatar.com/avatar/666e2844d622f8714e8ccf8ffb48a47c'
            'https://1.gravatar.com/avatar/666e2844d622f8714e8ccf8ffb48a47c'
            'https://0.gravatar.com/avatar/29701ae503ec7d9e670edaf095503067'
            'https://2.gravatar.com/avatar/605d445205b61ec11185a28dc4ab9323'
            'https://2.gravatar.com/avatar/68edef29d4c6826af22d6fcbbf8f1084'
        ]

        self.produce({
            'images-to-download': results,
        })


class Downloader(Step):
    import re

    def make_filename(self, index):
        original_url = self.payload['initial']['url']
        slug = re.sub(r'\W+', '-', original_url)
        return ".".join([slug.strip('-'), index,'.png'])

    def consume(self, instructions):
        images_to_download = instructions['images-to-download']
        filenames = []
        for index, image in enumerate(images_to_download):
            filename = self.make_filename(index)
            response = requests.get(image)

            with open(filename) as f:
                f.write(response.content)
                filenames.append(filename)

        self.produce({
            'filenames': filenames
        })
```

### Defining pipelines


```python
# myapp/pipelines.py

from lineup import Pipeline
from myapp.steps import Scraper, Downloader

class GravatarScraping(Pipeline):
    name = 'gravatars-from-github'

    steps = [
        Scraper,
        Downloader
    ]

```

### Running

```bash
lineup gravatars-from-github {'url': 'https://github.com/trending/developers'}
```


## Contributing

### Install Dependencies


```bash
curd install -r development.txt
```

### Run tests


```bash
make test
```

[![instanc.es Badge](https://instanc.es/bin/gabrielfalcao/lineup.png)](http://instanc.es)
