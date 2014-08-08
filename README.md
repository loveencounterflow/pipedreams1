

- [PipeDreams](#pipedreams)
	- [Motivation](#motivation)
	- [Overview](#overview)

> **Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*


## PipeDreams

Common operations for piped NodeJS streams.

`npm install --save pipedreams`

### Motivation

> **a stream is just a series of things over time**. if you were to re-implement your
> library to use your own stream implementation, you'd end up with an 80% clone
> of NodeJS core streams (and get 20% wrong). so why not just use core streams?—*paraphrased
> from Dominic Tarr, Nodebp April 2014: The History of Node.js Streams.*

So i wanted to read those huge [GTFS](https://developers.google.com/transit/gtfs/reference) files for
my nascent [TimeTable](https://github.com/loveencounterflow/timetable) project, and all went well
except for those *humongous* files with millions and millions of lines.

I stumbled over the popular [`csv-parse`](https://github.com/wdavidw/node-csv-parse#using-the-pipe-function)
package that is widely used by NodeJS projects, and, looking at the `pipe` interface, i found it
very enticing and suitable, so i started using it.

Unfortunately, it so turned out that i kept loosing records from my data. Most blatantly, some data sets
ended up containing a consistent number of 16384 records, although the sources contain many more records.
i've since found out that `csv-parse` has some issues related to stream backpressure not being handled
correctly
(see my [question on StackOverflow](http://stackoverflow.com/questions/25181441/how-to-work-with-large-files-nodejs-streams-and-pipes)
and the related [issue on GitHub]()).

More research revealed two things:

* NodeJS streams *can* be difficult to grasp. They're new, they're hot, they're much talked about but
  also somewhat underdocumented, their API is just shy of being convoluted. Streams are so hard to
  get right the NodeJS team saw it fit to introduce a second major version in 0.10.x—although
  streams had been part of NodeJS from very early on.

* More than a few projects out there provide software that use a non-core (?) stream implementation as part
  of their project and expose the relevant methods in their API; `csv-parse`
  is one of those, and hence its problems. Having looked at a few projects, i started to suspect that this
  is wrong: CSV-parser-with-streams-included libraries are often very specific in what they allow you to do, and, hence, limited;
  moreover, there is a tendency for those stream-related methods to eclipse what a CSV parser, at its core, should
  be good at (parsing CSV).

  Have a look at the [`fast-csv` API](http://c2fo.github.io/fast-csv/index.html)
  to see what i mean: you get a lot of `fastcsv.createWriteStream`, `fastcsv.fromStream` and so on methods.
  Thing is, you don't need that stuff to work with streams, and you don't need that stuff to parse
  CSV files, so those methods are simply superfluous.

  **A good modern NodeJS CSV parser should be
  *compatible* with streams, it should *not* replace or emulate NodeJS core streams—that is a violation
  of the principle of [Separation of Concerns (SoC)](http://en.wikipedia.org/wiki/Separation_of_concerns).**

### Overview

PipeDreams is a library that is built on top of Dominic Tarr's great
[event-stream](https://github.com/dominictarr/event-stream), which is "a toolkit to make creating and
working with streams easy".

PipeDreams—as the name implies—is centered around the pipeline model of working with streams. A quick
example is in place:

```coffee
@read_stop_times = ( registry, route, handler ) ->
  input       = P.create_readstream route, 'stop_times'
  input.pipe P.$split()
    .pipe P.$sample                     1 / 1e4, headers: true
    .pipe P.$skip_empty()
    .pipe P.$parse_csv()
    .pipe @$clean_stoptime_record()
    .pipe P.$set                        '%gtfs-type', 'stop_times'
    .pipe P.$delete_prefix              'trip_'
    .pipe P.$dasherize_field_names()
    .pipe P.$rename                     'id',             '%gtfs-trip-id'
    .pipe P.$rename                     'stop-id',        '%gtfs-stop-id'
    .pipe P.$rename                     'arrival-time',   'arr'
    .pipe P.$rename                     'departure-time', 'dep'
    .pipe @$add_stoptimes_gtfsid()
    .pipe @$register                    registry
    .on 'end', ->
      info 'ok: stoptimes'
      return handler null, registry
```


