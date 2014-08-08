# main.coffee


#### Functions
  
* [this.$skip\_empty](#this.$skip_empty)
  
* [this.$skip\_after](#this.$skip_after)
  
* [this.$sample](#this.$sample)
  
* [this.$batch](#this.$batch)
  
* [this.$collect\_sample](#this.$collect_sample)
  
* [this.$signal\_end](#this.$signal_end)
  
* [this.$on\_end](#this.$on_end)
  
* [this.$show\_and\_quit](#this.$show_and_quit)
  
* [this.$show\_table](#this.$show_table)
  
* [this.$show](#this.$show)
  
* [this.$count](#this.$count)
  
* [this.$parse\_csv](#this.$parse_csv)
  
* [this.$as\_pods](#this.$as_pods)
  
* [this.$delete\_prefix](#this.$delete_prefix)
  
* [this.$dasherize\_field\_names](#this.$dasherize_field_names)
  
* [this.\_rename](#this._rename)
  
* [this.\_copy](#this._copy)
  
* [this.$rename](#this.$rename)
  
* [this.$copy](#this.$copy)
  
* [this.$set](#this.$set)
  
* [get\_random\_integer](#get_random_integer)
  
* [rnd\_from\_seed](#rnd_from_seed)
  







## Functions
  
### <a name="this.$skip_empty">this.$skip\_empty()</a>

  
### <a name="this.$skip_after">this.$skip\_after(limit)</a>

  
### <a name="this.$sample">this.$sample(p, options)</a>
 Given a `0 <= p <= 1`, interpret `p` as the *p*robability to *p*ick a given record and otherwise toss
it, so that `$sample 1` will keep all records, `$sample 0` will toss all records, and
`$sample 0.5` (the default) will toss every other record.

You can pipe several `$sample()` calls, reducing the data stream to 50% with each step. If you know
your data set has, say, 1000 records, you can cut down to a random sample of 10 by piping the result of
calling `$sample 1 / 1000 * 10` (or, of course, `$sample 0.01`).

Tests have shown that a data file with 3'722'578 records (which didn't even fit into memory when parsed)
could be perused in a matter of seconds with `$sample 1 / 1e4`, delivering a sample of around 370
records. Because these records are randomly selected and because the process is so immensely sped up, it
becomes possible to develop regular data processing as well as coping strategies for data-overload
symptoms with much more ease as compared to a situation where small but realistic data sets are not
available or have to be produced in an ad-hoc, non-random manner.

**Parsing CSV**: There is a slight complication when your data is in a CSV-like format: in that case,
there is, with `0 < p < 1`, a certain chance that the *first* line of a file is tossed, but some
subsequent lines are kept. If you start to transform the text line into objects with named values later in
the pipe (which makessense, because you will typically want to thin out largeish streams as early on as
feasible), the first line kept will be mis-interpreted as a header line (which must come first in CSV
files) and cause all subsequent records to become weirdly malformed. To safeguard against this, use
`$sample p, headers: true` (JS: `$sample( p, { headers: true } )`) in your code.

**Predictable Samples**: Sometimes it is important to have randomly selected data where samples are
constant across multiple runs:

* once you have seen that a certain record appears on the screen log, you are certain it will be in the
  database, so you can write a snippet to check for this specific one;

* you have implemented a new feature you want to test with an arbitrary subset of your data. You're
  still tweaking some parameters and want to see how those affect output and performance. A random
  sample that is different on each run would be a problem because the number of records and the sheer
  bytecount of the data may differ from run to run, so you souldn't be sure which effects are due to
  which causes.

Use `$sample p, seed: 1234` with a non-zero number of your choice); you will then get the exact same
sample whenever you re-run your piping application with the same stream, the same seed, and the same
delta. An interesting property of the predictable sample is that a sample with a smaller `p` will always
be a subset of a sample with a bigger `p` and vice versa (provided seed and delta were constant). 
  
### <a name="this.$batch">this.$batch(n, handler)</a>
TAINT check for meaningful n 
  
### <a name="this.$collect_sample">this.$collect\_sample(input_stream, n, options, result_handler)</a>
 Given an `input_stream`, a positive integer number `n`, (facultatively) options, and a `handler`, try
to assemble a representative sample with up to `n` records from the stream. When the stream has ended,
the handler is called once with (a `null` error argument and) a list of records.

Similarly to `PIPEDREAMS.$sample`, it is possible to pass in a `headers: true` option to make sure the
headers line of a CSV file is not collected. Also similarly, a `seed: 1234` argument can be used to
ensure that the sample is arbitrary but constant for the same stream and the same seed.

Observe that while `$sample` does thin out the stream, `$collect_sample` will never add anything to or
omit anything from the stream; in that respect, it is rather more similar to `$collect`.

The (simple) algorithm this method uses to arrive at a representative, fixed-size sample from a collection
of unknown size has been kindly provided by two guys on
[Math StackExchange](http://math.stackexchange.com/q/890272/168522). 
  
### <a name="this.$signal_end">this.$signal\_end(signal)</a>
 Given an optional `signal` (which defaults to `null`), return a stream transformer that emits
`signal` as last value in the stream. Observe that whatever value you choose for `signal`, that value
should be gracefully handled by any transformers that follow in the pipe. 
  
### <a name="this.$on_end">this.$on\_end(handler)</a>

  
### <a name="this.$show_and_quit">this.$show\_and\_quit()</a>

  
### <a name="this.$show_table">this.$show\_table(input_stream)</a>
TAINT may introduce a memory leak. 
  
### <a name="this.$show">this.$show()</a>

  
### <a name="this.$count">this.$count(handler)</a>
TAINT make signal configurable 
  
### <a name="this.$parse_csv">this.$parse\_csv()</a>

  
### <a name="this.$as_pods">this.$as\_pods()</a>

  
### <a name="this.$delete_prefix">this.$delete\_prefix(prefix)</a>

  
### <a name="this.$dasherize_field_names">this.$dasherize\_field\_names()</a>

  
### <a name="this._rename">this.\_rename(record, old_field_name, new_field_name)</a>

  
### <a name="this._copy">this.\_copy(record, old_field_name, new_field_name, action)</a>

  
### <a name="this.$rename">this.$rename(old_field_name, new_field_name)</a>

  
### <a name="this.$copy">this.$copy(old_field_name, new_field_name)</a>

  
### <a name="this.$set">this.$set(field_name, field_value)</a>

  
### <a name="get_random_integer">get\_random\_integer(rnd, min, max)</a>

  
### <a name="rnd_from_seed">rnd\_from\_seed(seed)</a>

  

  
    
  

