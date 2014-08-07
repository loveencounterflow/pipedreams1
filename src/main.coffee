

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
### http://c2fo.github.io/fast-csv/index.html, https://github.com/C2FO/fast-csv ###
S                         = require 'string'
#...........................................................................................................
@create_readstream        = require './create-readstream'


############################################################################################################
# GENERIC METHODS
#-----------------------------------------------------------------------------------------------------------
@$        = ES.map.bind ES
@$split   = ES.split.bind ES




############################################################################################################
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
    return handler() if count > limit
    count += 1
    handler null, record


#===========================================================================================================
# SAMPLING / THINNING OUT
#-----------------------------------------------------------------------------------------------------------
@$sample = ( p = 0.5, options ) ->
  ### Given a `0 <= p <= 1`, interpret `p` as the *p*robability to *p*ick a given record and otherwise toss
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

  **Parsing CVS**: There is a slight complication when your data is in a CXV-like format: in that case,
  there is, with `0 < p < 1`, a certain chance that the *first* line of a file is tossed, but some
  subsequent lines are kept. If you start to transform the text line into objects with named values later in
  the pipe (which makessense, because you will typically want to thin out largeish streams as early on as
  feasible), the first line kept will be mis-interpreted as a header line (which must come first in CVS
  files) and cause all subsequent records to become weirdly malformed. To safeguard against this, use
  `$sample p, headers: true` (JS: `$sample( p, { headers: true } )`) in your code.

  **Predictable Samples**: Sometimes it is important to have randomly selected data where samples are
  constant across multiple runs (e.g. because once you have seen that a certain record appears on the screen
  log, you are certain it will be in the database, so you can write a snippet to search for this specific
  one). In such a case, use `$sample p, seed: 1234` with a non-zero number of your choice); you will then
  get the exact same sample whenever you re-run your piping application with the same stream, the same seed,
  and the same delta. An interesting property of the predictable sample is that a sample with a smaller `p`
  will always be a subset of a sample with a bigger `p` and vice versa (provided seed and delta were
  constant). ###
  #.........................................................................................................
  unless 0 <= p <= 1
    throw new Error "need a probability between 0 and 1, got rpr #{p}"
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



############################################################################################################
# COLLECTING
#-----------------------------------------------------------------------------------------------------------
@$collect = ( input_stream, n, result_handler ) ->
  collector = []
  input_stream.on 'end', =>
    result_handler null, collector if collector.length > 0
    result_handler null, null
  #.........................................................................................................
  return @$ ( record, handler ) ->
    collector.push record
    #.......................................................................................................
    if collector.length >= n
      result_handler null, collector
      collector = []
    #.......................................................................................................
    handler null, collector

#-----------------------------------------------------------------------------------------------------------
@$collect_sample = ( input_stream, n, options, result_handler ) ->
  ### Given an `input_stream`, a positive integer number `n`, (facultatively) options, and a `handler`, try
  to assemble a representative sample with up to `n` records from the stream. When the stream has ended,
  the handler is called once with (a `null` error argument and) a list of records.

  Similarly to `PIPEDREAMS.$sample`, it is possible to pass in a `headers: true` option to make sure the
  headers line of a CVS file is not collected. Also similarly, a `seed: 1234` argument can be used to
  ensure that the sample is arbitrary but constant for the same stream and the same seed.

  Observe that while `$sample` does thin out the stream, `$collect_sample` will never add anything to or
  omit anything from the stream; in that respect, it is rather more similar to `$collect`.

  The (simple) algorithm this method uses to arrive at a representative, fixed-size sample from a collection
  of unknown size has been kindly provided by two guys on
  [Math StackExchange](http://math.stackexchange.com/q/890272/168522). ###
  switch arity = arguments.length
    when 3
      result_handler  = options
      headers         = false
      seed            = null
    when 4
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
  input_stream.once 'end', => result_handler null, collector
  #.........................................................................................................
  return @$ ( record, handler ) =>
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
        if rnd_pick() >= p
          collector[ get_random_integer rnd_idx, 0, n - 1 ] = record
    #.......................................................................................................
    handler null, record



############################################################################################################
# DISPLAY / REPORTING
#-----------------------------------------------------------------------------------------------------------
@$show_and_quit = ->
  return @$ ( record, handler ) =>
    info rpr record
    warn 'aborting from `TRANSFORMERS.show_and_quit`'
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
@$show_sample = ( input_stream ) ->
  ### TAINT may introduce a memory leak. ###
  records = []
  input_stream.once 'end', =>
    whisper '©5r0', "displaying random record out of #{records.length}"
    debug   '©5r0', rpr records[ Math.floor Math.random() * records.length ]
  return @$ ( record, handler ) =>
    records.push record
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@$count = ( input_stream, title ) ->
  count = 0
  input_stream.on 'end', ->
    urge ( title ? 'Count' ) + ':', count
  return @$ ( record, handler ) =>
    count += 1
    handler null, record


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


#===========================================================================================================
# HELPERS
#-----------------------------------------------------------------------------------------------------------
get_random_integer = ( rnd, min, max ) ->
  return ( Math.floor rnd() * ( max + 1 - min ) ) + min

#-----------------------------------------------------------------------------------------------------------
rnd_from_seed = ( seed ) ->
  return if seed? then ( require 'coffeenode-bitsnpieces' ).get_rnd seed else Math.random

