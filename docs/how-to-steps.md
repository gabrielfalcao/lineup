# Introduction to Steps

Steps must are classes that inherit from `lineup.steps.Step`, there are a few methods you can overwrite and get a nice level of customization and control of your pipeline.

```python
import requests
from lineup import Step


class Downloader(Step):
    def before_consume(self):
        self.log("%s is about to consume its queue", self.name)

    def after_consume(self, instructions):
        self.log("%s is done", self.name)

    def consume(self, instructions):
        response = requests.get(instructions['url'])
        instructions['response'] = {
            'content': response.content,
            'status_code': response.status_code,
        }
        self.produce(instructions)

    def rollback(self, instructions):
        # What to do when "consume" failed ?
        # In this case I'll check the number of attempts
        if 'attempts' not in instructions:
            instructions['attempts'] = 0

        # Fail after 3 attempts
        elif instructions['attempts'] > 3:
            raise RuntimeError('Could not download %s', instructions['url'])

        instructions['attempts'] += 1
```
