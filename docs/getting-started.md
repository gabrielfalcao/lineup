# A Basic pipeline

The simples pipeline has only one step, and produces a result


```python
import logging
from lineup import Step
from lineup import Pipeline

logger = logging.getLogger()


class SimpleForwarding(Step):
    def consume(self, instructions):
        logger.info("Success: %s", instructions['message'])
        self.produce({'produced': instructions})

class HelloWorld(Pipeline):
    name = 'hello-world'
    steps = [SimpleForwarding]
```

## Running it

LineUp auto discovers pipelines declared in python files: run lineup form a root folder where pipelines can be found, or pass the `--working-dir=/path/to/pipelines`

```bash
lineup hello-world run
```

## Enqueuing items

```bash
lineup hello-world push {"message": "Hello World"}
```


Learn more about [how steps work](how-to-steps.md), or find out more about the [command line tool](how-to-cli.md).
