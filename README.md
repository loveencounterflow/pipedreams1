

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
  also somewhat underdocumented, and their API is just shy of being convoluted. Streams are so hard to
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
(CoffeeScript) example is in place:

```coffee
P = require 'pipedreams'                                                  #  1
                                                                          #  2
@read_stop_times = ( registry, route, handler ) ->                        #  3
  input = P.create_readstream route, 'stop_times'                         #  4
  input.pipe P.$split()                                                   #  5
    .pipe P.$sample                     1 / 1e4, headers: true            #  6
    .pipe P.$skip_empty()                                                 #  7
    .pipe P.$parse_csv()                                                  #  8
    .pipe @$clean_stoptime_record()                                       #  9
    .pipe P.$set                        '%gtfs-type', 'stop_times'        # 10
    .pipe P.$delete_prefix              'trip_'                           # 11
    .pipe P.$dasherize_field_names()                                      # 12
    .pipe P.$rename                     'id',             '%gtfs-trip-id' # 13
    .pipe P.$rename                     'stop-id',        '%gtfs-stop-id' # 14
    .pipe P.$rename                     'arrival-time',   'arr'           # 15
    .pipe P.$rename                     'departure-time', 'dep'           # 16
    .pipe @$add_stoptimes_gtfsid()                                        # 17
    .pipe @$register                    registry                          # 18
    .on 'end', ->                                                         # 19
      info 'ok: stoptimes'                                                # 20
      return handler null, registry                                       # 21
```

i agree that there's a bit of line noise here, so let's rewrite that piece in cleaned-up pseudo-code:

```coffee
P = require 'pipedreams'                                                  #  1
                                                                          #  2
read_stop_times = ( registry, route, handler ) ->                         #  3
  input = create_readstream route, 'stop_times'                           #  4
    | split()                                                             #  5
    | sample                     1 / 1e4, headers: true                   #  6
    | skip_empty()                                                        #  7
    | parse_csv()                                                         #  8
    | clean_stoptime_record()                                             #  9
    | set                        '%gtfs-type', 'stop_times'               # 10
    | delete_prefix              'trip_'                                  # 11
    | dasherize_field_names()                                             # 12
    | rename                     'id',             '%gtfs-trip-id'        # 13
    | rename                     'stop-id',        '%gtfs-stop-id'        # 14
    | rename                     'arrival-time',   'arr'                  # 15
    | rename                     'departure-time', 'dep'                  # 16
    | add_stoptimes_gtfsid()                                              # 17
    | register                    registry                                # 18
    .on 'end', ->                                                         # 19
      info 'ok: stoptimes'                                                # 20
      return handler null, registry                                       # 21
```

What happens here is, roughly: `input` is a PipeDreams ReadStream object created as `create_readstream route,
label`. PipeDreams ReadStreams are nothing but what NodeJS gives you with `fs.createReadStream`; they're
just a bit pimped so you get a [nice progress bar on the console](https://github.com/visionmedia/node-progress)
which is great because those files can take *minutes* to process completely, and it's nasty to stare at
a silent command line that doesn't keep you informed what's going on. Having a progress bar pop up is
great because i used to report progress numbers manually, and now i get a better solution for free.

On line #5, we put a `split` operation (as `P.$split()`) into the pipeline, which is just
`eventstream.split()` and splits whatever is read from the file into (chunks that are) lines. You do not
want that if you read, say, `blockbusters.avi` from the disk, but you certainly want that if you're
reading `all-instances-where-a-bus-stopped-at-a-bus-stop-in-northeast-germany-in-fall-2014.csv`, which,
if left unsplit, is an unwieldy *mass* of data.




