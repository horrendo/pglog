# pglog

A lot of useful information can be obtained from Postgres [csv-formatted logs](https://www.postgresql.org/docs/12/runtime-config-logging.html#RUNTIME-CONFIG-LOGGING-CSVLOG). This shard aims to make that process easier.

[![Build Status](https://travis-ci.org/horrendo/pglog.svg?branch=master)](https://travis-ci.org/horrendo/pglog)

## Installation

1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     pglog:
       github: horrendo/pglog
   ```

2. Run `shards install`

## Usage

```crystal
require "pglog"
```

You can access the documentation [here](https://horrendo.github.io/pglog/)

## Contributing

1. Fork it (<https://github.com/horrendo/pglog/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

- [Steve Baldwin](https://github.com/horrendo) - creator and maintainer
