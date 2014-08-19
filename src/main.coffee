

############################################################################################################
njs_util                  = require 'util'
#...........................................................................................................
TEXT                      = require 'coffeenode-text'
TRM                       = require 'coffeenode-trm'
rpr                       = TRM.rpr.bind TRM
badge                     = 'PIPEDREAMS/main'
log                       = TRM.get_logger 'plain',     badge
info                      = TRM.get_logger 'info',      badge
whisper                   = TRM.get_logger 'whisper',   badge
alert                     = TRM.get_logger 'alert',     badge
debug                     = TRM.get_logger 'debug',     badge
warn                      = TRM.get_logger 'warn',      badge
help                      = TRM.get_logger 'help',      badge
urge                      = TRM.get_logger 'urge',      badge
echo                      = TRM.echo.bind TRM
rainbow                   = TRM.rainbow.bind TRM
#...........................................................................................................
### https://github.com/dominictarr/event-stream ###
ES                        = require 'event-stream'
#...........................................................................................................
### http://stringjs.com ###
S                         = require 'string'
#...........................................................................................................
HELPERS                   = require './HELPERS'


#===========================================================================================================
# GENERIC METHODS
#-----------------------------------------------------------------------------------------------------------
@create_readstream  = HELPERS.create_readstream .bind HELPERS
@pimp_readstream    = HELPERS.pimp_readstream   .bind HELPERS
@$                  = ES.map      .bind ES
@map                = ES.mapSync  .bind ES
@merge              = ES.merge    .bind ES
@$split             = ES.split    .bind ES
@$chain             = ES.pipeline .bind ES
@through            = ES.through  .bind ES
@duplex             = ES.duplex   .bind ES
@as_readable        = ES.readable .bind ES
@read_list          = ES.readArray.bind ES
@eos                = { 'eos': true }


#===========================================================================================================
# DELETION
#-----------------------------------------------------------------------------------------------------------
@$skip_empty = ->
  return @$ ( record, handler ) =>
    return handler() if record.length is 0
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@$skip_after = ( limit = 1 ) ->
  count = 0
  return @$ ( record, handler ) =>
    count += 1
    return handler() if count > limit
    handler null, record


#===========================================================================================================
# SAMPLING / THINNING OUT
#-----------------------------------------------------------------------------------------------------------
@$sample = ( p = 0.5, options ) ->
  ### Given a `0 <= p <= 1`, interpret `p` as the *p*robability to *p*ick a given record and otherwise toss
  it, so that `$sample 1` will keep all records, `$sample 0` will toss all records, and
  `$sample 0.5` (the default) will toss (on average) every other record.

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
  the pipe (which makes sense, because you will typically want to thin out largeish streams as early on as
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
    bytecount of the data may differ from run to run, so you wouldn't be sure which effects are due to
    which causes.

  To obtain predictable samples, use `$sample p, seed: 1234` (with a non-zero number of your choice);
  you will then get the exact same
  sample whenever you re-run your piping application with the same stream and the same seed. An interesting
  property of the predictable sample is that—everything else being the same—a sample with a smaller `p`
  will always be a subset of a sample with a bigger `p` and vice versa. ###
  #.........................................................................................................
  unless 0 <= p <= 1
    throw new Error "expected a number between 0 and 1, got #{rpr p}"
  #.........................................................................................................
  ### Handle trivial edge cases faster (hopefully): ###
  return ( @$ ( record, handler ) => handler null, record ) if p == 1
  return ( @$ ( record, handler ) => handler()            ) if p == 0
  #.........................................................................................................
  headers = options?[ 'headers'     ] ? false
  seed    = options?[ 'seed'        ] ? null
  count   = 0
  rnd     = rnd_from_seed seed
  #.........................................................................................................
  return @$ ( record, handler ) =>
    count += 1
    return handler null, record if ( count is 1 and headers ) or rnd() < p
    handler()



#===========================================================================================================
# COLLECTING
#-----------------------------------------------------------------------------------------------------------
@$batch = ( n, handler ) ->
  ### TAINT check for meaningful n ###
  ### TAINT make signal configurable ###
  collector     = []
  received_eos  = false
  eos           = @eos
  #.........................................................................................................
  if handler?
    #.........................................................................................................
    on_data = ( record ) ->
      if record is eos
        received_eos  = true
      else
        collector.push record
        if collector.length >= n
          handler null, collector
          collector = []
      @emit 'data', record
    #.........................................................................................................
    on_end  = ->
      handler null, collector if collector.length > 0
      handler null, eos       if received_eos
      @emit 'end'
  #.........................................................................................................
  else
    #.........................................................................................................
    on_data = ( record ) ->
      if record is eos
        received_eos  = true
      else
        collector.push record
        if collector.length >= n
          @emit 'data', collector
          collector = []
    #.........................................................................................................
    on_end  = ->
      @emit 'data', collector if collector.length > 0
      @emit 'data', eos       if received_eos
      @emit 'end'
  #.........................................................................................................
  return ES.through on_data, on_end

#-----------------------------------------------------------------------------------------------------------
@$collect_sample = ( n, options, result_handler ) ->
  ### Given an `input_stream`, a positive integer number `n`, (facultatively) options, and a `handler`, try
  to assemble a representative sample with up to `n` records from the stream. When the stream has ended,
  the handler is called once with (a `null` error argument and) a list of records.

  Similarly to `PIPEDREAMS.$sample`, it is possible to pass in a `headers: true` option to make sure the
  headers line of a CSV file is not collected. Also similarly, a `seed: 1234` argument can be used to
  ensure that the sample is arbitrary but constant for the same stream and the same seed.

  Observe that while `$sample` does thin out the stream, `$collect_sample` will never add anything to or
  omit anything from the stream; in that respect, it is rather more similar to `$collect`.

  The (simple) algorithm this method uses to arrive at a representative, fixed-size sample from a collection
  of unknown size has been kindly provided by two guys on
  [Math StackExchange](http://math.stackexchange.com/q/890272/168522). ###
  switch arity = arguments.length
    when 2
      result_handler  = options
      headers         = false
      seed            = null
    when 3
      headers         = options[ 'headers'  ] ? false
      seed            = options[ 'seed'     ] ? null
    else
      throw new Error "expected 2 or 3 arguments, got #{arity}"
  #.........................................................................................................
  if n <= 0 or n != Math.floor n
    throw new Error "expected a positive non-zero integer, got #{n}"
  #.........................................................................................................
  idx       = -1
  m         = 0
  p         = 1
  rnd_pick  = rnd_from_seed seed
  rnd_idx   = rnd_from_seed seed
  collector = []
  #.........................................................................................................
  on_data = ( record ) ->
    ### thx to http://math.stackexchange.com/a/890284/168522,
    http://math.stackexchange.com/a/890285/168522 ###
    idx += 1
    #.......................................................................................................
    unless idx is 0 and headers
      m += 1
      if m <= n
        collector.push record
      else
        p = n / m
        if rnd_pick() < p
          collector[ get_random_integer rnd_idx, 0, n - 1 ] = record
    #.......................................................................................................
    @emit 'data', record
  #.........................................................................................................
  on_end = ->
    result_handler null, collector
    @emit 'end'
  #.........................................................................................................
  return ES.through on_data, on_end


#===========================================================================================================
# STREAM END DETECTION
#-----------------------------------------------------------------------------------------------------------
@$signal_end = ( signal = @eos ) ->
  ### Given an optional `signal` (which defaults to `null`), return a stream transformer that emits
  `signal` as last value in the stream. Observe that whatever value you choose for `signal`, that value
  should be gracefully handled by any transformers that follow in the pipe. ###
  on_data = null
  on_end  = ->
    @emit 'data', signal
    @emit 'end'
  return ES.through on_data, on_end

#-----------------------------------------------------------------------------------------------------------
@$on_end = ( handler ) ->
  on_end = ->
    handler null, null
    @emit 'end'
  return ES.through null, on_end



#===========================================================================================================
# OBJECT CONVERSION
#-----------------------------------------------------------------------------------------------------------
@$value_from_key = ( x ) ->
  return @$ ( key, handler ) ->
    handler null, x[ key ]

#-----------------------------------------------------------------------------------------------------------
@$read_values = ( x ) ->
  ### TAINT produces an intermediate list of object keys ###
  return @$chain ( @read_list Object.keys x ), ( @$value_from_key x )

# #-----------------------------------------------------------------------------------------------------------
# @read_keys = ( x ) ->
#   return @as_readable ( count, handler ) ->
#     for key of x
#       handler null, key
#     @emit 'end'

# #-----------------------------------------------------------------------------------------------------------
# @read_values = ( x ) ->
#   return @as_readable ( count, handler ) ->
#     for _, value of x
#       handler null, value
#     @emit 'end'

# #-----------------------------------------------------------------------------------------------------------
# @read_facets = ( x ) ->
#   return @as_readable ( count, handler ) ->
#     for key, value of x
#       handler null, [ key, value, ]
#     @emit 'end'



#===========================================================================================================
# DISPLAY / REPORTING
#-----------------------------------------------------------------------------------------------------------
@$show_and_quit = ->
  return @$ ( record, handler ) =>
    info rpr record
    warn 'aborting from `PIPEDREAMS.show_and_quit`'
    setImmediate -> process.exit()
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@$show_table = ( input_stream ) ->
  ### TAINT may introduce a memory leak. ###
  EASYTABLE = require 'easy-table'
  records = []
  input_stream.once 'end', =>
    echo EASYTABLE.printArray records
  return @$ ( record, handler ) =>
    records.push record
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@$show = ->
  return @$ ( record, handler ) =>
    info rpr record
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@$count = ( handler ) ->
  ### TAINT make signal configurable ###
  ### Given an optional `handler`, issue a count of records upon stream completion.

  When a handler is given, then it is called the NodeJS way as `handler null, count`; all records are passed
  through untouched and may be consumed by downstream transformers.

  Conversely, when no handler is given, then `$count` acts as an aggregator: all records are silently
  tossed, and the only item left in the pipe is the count.

  In any event, if a PipeDreams End-Of-Stream value is detected in the pipe, it is passed through and not
  counted; when `$count` is called without a handler, then the EOS will be sent *after* the count. ###
  eos           = @eos
  received_eos  = false
  count         = 0
  #.........................................................................................................
  if handler?
    #.......................................................................................................
    on_data = ( record ) ->
      if record is eos
        received_eos = true
      else
        count += 1
        @emit 'data', record
    #.......................................................................................................
    on_end = ->
      handler null, count
      @emit 'data', eos if received_eos
      @emit 'end'
  #.........................................................................................................
  else
    #.......................................................................................................
    on_data = ( record ) ->
      if record is eos
        received_eos = true
      else
        count += 1
    #.......................................................................................................
    on_end = ->
      @emit 'data', count
      @emit 'data', eos if received_eos
      @emit 'end'
  #.........................................................................................................
  return ES.through on_data, on_end


#===========================================================================================================
# CSV
#-----------------------------------------------------------------------------------------------------------
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


#===========================================================================================================
# PODs
#-----------------------------------------------------------------------------------------------------------
@$as_pods = ->
  record_idx  = -1
  field_names = null
  #.........................................................................................................
  return @$ ( record, handler ) =>
    # whisper record.join ''
    if ( record_idx += 1 ) is 0
      field_names = record
      return
    R = {}
    for field_value, field_idx in record
      field_name      = field_names[ field_idx ]
      R[ field_name ] = field_value
    handler null, R

#-----------------------------------------------------------------------------------------------------------
@$delete_prefix = ( prefix ) ->
  if njs_util.isRegExp prefix then  starts_with = ( text, prefix ) -> prefix.test text
  else                              starts_with = ( text, prefix ) -> ( text.lastIndexOf prefix, 0 ) is 0
  #.........................................................................................................
  return @$ ( record, handler ) =>
    for old_field_name, field_value of record
      continue unless starts_with old_field_name, prefix
      new_field_name =  old_field_name.replace prefix, ''
      continue if new_field_name.length is 0
      ### TAINT should throw error ###
      continue if record[ new_field_name ]?
      record[ new_field_name ] = field_value
      delete record[ old_field_name ]
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@$dasherize_field_names = ->
  return @$ ( record, handler ) =>
    for old_field_name of record
      new_field_name = old_field_name.replace /_/g, '-'
      continue if new_field_name is old_field_name
      @_rename record, old_field_name, new_field_name
    #.......................................................................................................
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@_rename = ( record, old_field_name, new_field_name ) ->
  @_copy record, old_field_name, new_field_name, 'rename'
  delete record[ old_field_name ]
  return record

#-----------------------------------------------------------------------------------------------------------
@_copy = ( record, old_field_name, new_field_name, action ) ->
  #.........................................................................................................
  if record[ old_field_name ] is undefined
    error = new Error """
      when trying to #{action} field #{rpr old_field_name} to #{rpr new_field_name}
      found that there is no field #{rpr old_field_name} in
      #{rpr record}"""
    error[ 'code' ] = 'no such field'
    throw error
  #.........................................................................................................
  if record[ new_field_name ] isnt undefined
    throw new Error """
      when trying to #{action} field #{rpr old_field_name} to #{rpr new_field_name}
      found that field #{rpr new_field_name} already present in
      #{rpr record}"""
    error[ 'code' ] = 'duplicate field'
    throw error
  #.........................................................................................................
  record[ new_field_name ] = record[ old_field_name ]
  return record

#-----------------------------------------------------------------------------------------------------------
@$rename = ( old_field_name, new_field_name ) ->
  return @$ ( record, handler ) =>
    handler null, @_rename record, old_field_name, new_field_name

#-----------------------------------------------------------------------------------------------------------
@$copy = ( old_field_name, new_field_name ) ->
  return @$ ( record, handler ) =>
    handler null, @_copy record, old_field_name, new_field_name, 'copy'

#-----------------------------------------------------------------------------------------------------------
@$set = ( field_name, field_value ) ->
  return @$ ( record, handler ) =>
    record[ field_name ] = field_value
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@$pick = ( field_name ) ->
  return @$ ( record, handler ) =>
    value = record[ field_name ]
    return handler new Error "field #{rpr field_name} not defined in #{rpr record}" if value is undefined
    handler null, value


#===========================================================================================================
# HELPERS
#-----------------------------------------------------------------------------------------------------------
get_random_integer = ( rnd, min, max ) ->
  return ( Math.floor rnd() * ( max + 1 - min ) ) + min

#-----------------------------------------------------------------------------------------------------------
rnd_from_seed = ( seed ) ->
  return if seed? then ( require 'coffeenode-bitsnpieces' ).get_rnd seed else Math.random

