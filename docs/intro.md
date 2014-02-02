# Welcome to LineUp

Lineup is a redis-based
[pipeline](http://en.wikipedia.org/wiki/Pipeline_(software)) framework
that turns horizontal scalling seamless.

It's currently providing parallelism through python threads and is
pretty useful for writing systems where the workers make lots of
network I/O.

It scales horizontally, so you can simply run more workers in as many
machines you want.

Let's [get it started](getting-started.md)
