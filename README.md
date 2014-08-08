

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
    .pipe P.$set                        '%gtfs-type',     'stop_times'    # 10
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
    | set                        '%gtfs-type',      'stop_times'          # 10
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

What happens here is, roughly:

* On **line #4**, `input` is a PipeDreams ReadStream object created as `create_readstream route,
label`. PipeDreams ReadStreams are nothing but what NodeJS gives you with `fs.createReadStream`; they're
just a bit pimped so you get a [nice progress bar on the console](https://github.com/visionmedia/node-progress)
which is great because those files can take *minutes* to process completely, and it's nasty to stare at
a silent command line that doesn't keep you informed what's going on. Having a progress bar pop up is
great because i used to report progress numbers manually, and now i get a better solution for free.

* On **line #5**, we put a `split` operation (as `P.$split()`) into the pipeline, which is just
`eventstream.split()` and splits whatever is read from the file into (chunks that are) lines. You do not
want that if you're reading, say, `blockbusters.avi` from the disk, but you certainly want that if you're
reading `all-instances-where-a-bus-stopped-at-a-bus-stop-in-northeast-germany-in-fall-2014.csv`, which,
if left unsplit, is an unwieldy *mass* of data. As the CSV format mandates an optional header line and
one record per line of text, splitting into lines is a good preparation for getting closer to the data.

> For those who have never worked with streams or piping, observe that we have a pretty declarative interface
> here that does not readily reveal *how* things are done and on *which* arguments. That's great for building
> an abstraction—the code looks a lot like a Table of Contents where actions are labeled (and not
> described in detail), but it can be hard to wrap one's mind around. Fear you not, we'll have a look at some
> sample methods later on; those are pretty straightforward. Believe me when i say **you don't have to pass
> an exam on the [gritty details of the NodeJS Streams API](http://nodejs.org/api/stream.html) to use
> PipeDreams**.
>
> For the moment being, it's just important to know that what is passed between line #4
> `input = ...` and line #5 `split` are some arbitrarily-sized chunks of binary data which get transformed
> into chunks of line-sized text and passed into line #6 `sample ...`. The basic idea is that each step
> does something small / fast / elementary / generic do whatever it receives from above, and passes the result
> to the next stop in the pipe.

* On **line #6**, we have `P.$sample 1 / 1e4, headers: true` (for non-CS-folks:
`P.$sample( 1 / 1e4, { headers: true} )`). Let's dissect that one by one:

  * `P`, of course, is simply the
    result of `P = require 'pipedreams'`. I'm not much into abbreviations in coding, but since this
    particular reference will appear, like, *all* over the place, let's make it a snappy one.

  * `$sample` is a method of `P`. I adopt the convention of prefixing all methods that are suitable as an
    argument to a `pipe` method with `$`. This is to signal that **not `sample` itself, but rather its
    return value** should be put into the pipe. When you start to write your own pipes, you will often
    unadvertently write `input_A.pipe f`, `input_B.pipe f` and you'll have a problem: typically you do not
    want to share state between two unrelated streams, so each stream must get its unique pipe members.
    **Your piping functions are all piping function producers**—higher-order functions, that is. The
    `$` sigil is there to remind you of that: *$ == 'you must call this function in order to get the function
    you want in the pipe'*.

  * What does `$sample` do?—From the documentation:

    > Given a `0 <= p <= 1`, interpret `p` as the <b>P</b>robability to <b>P</b>ick a given record and otherwise toss
    > it, so that `$sample 1` will keep all records, `$sample 0` will toss all records, and
    > `$sample 0.5` (the default) will toss (on average) every other record.

    In other words, the argument `1 / 1e4` signals: pick one out of 10'000 records, toss (delete / skip
    / omit / forget / drop / ignore, you get the idea) everything else. The use of the word 'record' is
    customary here; in fact, it means 'whatever you get passed as data when called'. That could be
    a CSV record, a line of text, a number, a list of values, anything. `$sample`, like many PipeDreams
    methods, is fully generic and agnostic. Just as the quote above says, "a stream is just a series of things over time".
    We've just split a binary stream into lines of text with the previous step, so a 'record' at this
    particular point is just that, a line of text. Move `$sample` two steps downstream, and it'll get to see a
    parsed CSV record instead.

    Now the file that is being read here happens to contain 3'722'578 records, and this is why there's that
    `$sample` command (and why it is place in front of the actual CSV parsing): to fully process every
    single record takes minutes, which is tedious for
    testing. When a record is tossed, none of the ensuing pipe methods get anything to work on; this
    reduces minutes of processing to seconds. Of course, you do not get the full amount of data, but you do get to work
    on a representative sample, which is invaluable for developing (you can even make it so that the
    random sample stays the *same* across runs, which can also be important).—You probably want to make
    the current ratio (here: `1 / 1e4`) a configuration variable that is set to `1` in production.

    The second argument to `$sample`, `headers: true`, is there to ensure `$sample` won't accidentally
    toss out the CSV header with the field names, as that would damage the data.

> It's already becoming clear that PipeDreams is centered around two things: parsing CSV files, and
> dealing with big files. This is due to the circumstances leading to its creation. That said, i try
> to keep it as open and general as to be of use, if only as a template, for other, stream-related
> purposes.

  * On **line #7**, it's `P.$skip_empty()`. Not surprisingly, this step eliminates all empty lines. On
    second thought, that step should appear in front of the call to `$sample`, don't you think?

  * On **line #8**, it's time to `P.$parse_csv()`. For those among us who are good at digesting CoffeeScript,
    here is the implementation; you can see it's indeed quite straightforward:

    ```coffee
    ### http://stringjs.com/ ###
    S = require 'string'

    @$parse_csv = ->
      field_names = null
      return @$ ( record, handler ) =>
        values = ( S record ).parseCSV ',', '"', '\\'
        if field_names is null
          field_names = values
          return handler()
        record = {}
        record[ field_names[ idx ] ] = value for value, idx in values
        handler null, record
    ```
    For pure-JS aficionados, the outline of that is, basically,

    ```js
    this.$parse_csv = function() {
      var field_names = null;
      return this.$( function( record, handler ) {
        ...
        })
      }
    ```
    which makes it clear that `$parse_csv` is a function that returns a function. Incidentally, it also
    keeps some state in its closure, as `field_names` is bound to become a list of names the moment that
    the pipeline hits the first line of the file. This clarifies what we talked about earlier: you do
    not want to share this state across streams—one stream has one set of CSV headers, another stream
    another set. This is why it's so important to individualize members of a stream's pipe.

    > It's also quite clear that this implementation is both quick and dirty: it assumes the CSV does have
    > headers, that fields are separated by commas, strings may be surrounded by double quotes, and so on.
    > Those details should really be made configurable, whis hasn't yet happened here. Again, the moment
    > you call `P.$parse_csv` would be a perfect moment to fill out those details and get a bespoke
    > method that suits the needs at hand.

    One more important detail: the `record` that comes into (the function returned by) `$parse_csv` is
    a line of text; the `record` that goes out of it is a plain old object with named values. All the
    pipe member functions work in essentially this way: they accept whatever they're wont to accept and
    return whatever they see fit.

    > ...which puts a finger on another sore spot, the glaring absence of meaningful type-checking and
    > error handling in this model function.

Now let's dash a little faster across the remaining lines:

  * On **lines #9—#18**,

    ```coffee
    .pipe @$clean_stoptime_record()                                       #  9
    .pipe P.$set                        '%gtfs-type', 'stop_times'        # 10
    .pipe P.$delete_prefix              'trip_'                           # 11
    .pipe P.$dasherize_field_names()                                      # 12
    .pipe P.$rename                     'id', '%gtfs-trip-id'             # 13
    # ...
    .pipe @$add_stoptimes_gtfsid()                                        # 17
    .pipe @$register                    registry                          # 18
    ```

    we (**#9**) clean the record of unwanted fields (there are quite a few in the data); then we (**#10**)
    set a field `%gtfs-type` to value `'stop_times'` (the same for all records in the pipeline). Next
    (**#11**) we delete a redundant field name prefix using a PipeDreams method, (**#12**) change all the
    underscored field names to dashed style, (**#13**) rename a field and then some, (**#17**) we call
    another custom method to add an ID field and, finally, on **line #18**, we register the record in
    a registry.



