# products-data-converter

Script to convert CSV exports to TTL according to the price management data model.

## How-to
### Run the conversion

Put all CSV input files in `./data/input`.

Start a docker container with the current folder mounted.
```bash
docker run --rm -it -v `pwd`:/app -w /app ruby:2.5 /bin/bash
```

Inside the docker container, install gem dependencies and run the script
```bash
bundle install
ruby app.rb
```

Output file will be written to `./data/output`.
